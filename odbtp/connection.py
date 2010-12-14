# Copyright (c) 2010 Michael Saavedra

from ctypes import *

from odbtp.errors import *
from odbtp.types import *
from odbtp.constants import *

odb = cdll.LoadLibrary('libodbtp.so')
odb.odbWinsockStartup()

def connect(connect_string, server, port=2799):
    return Connection(connect_string, server, port)

class Connection:
    """Object representing a connection to the ODBTP server.
    """
    def __init__(self, connect_string, server, port=2799):
        self.open = False
        self.handle = odb.odbAllocate(None)
        if not self.handle:
            raise get_exception(self.handle)
        if not odb.odbLogin(
                self.handle,
                server,
                port,
                ODB_LOGIN_SINGLE,
                connect_string
                ):
            raise get_exception(self.handle)
        self.driver = self._get_driver()
        self._set_attributes()
        self.open = True
        self.committed = False
    
    def __del__(self):
        """Clean up if the user doesn't close the connection
        """
        if self.open:
            self.close()
    
    def close(self):
        """Close the connection now.
        
        The connection will be unusable from this point forward; an Error
        (or subclass) exception will be raised if any operation is attempted
        with the connection. The same applies to all cursor objects trying
        to use the connection.  Note that closing a connection without
        committing the changes first will cause an implicit rollback to be
        performed.
        """
        self._assert_connection_is_open()
        if not self.committed:
            self.rollback()
        self.open = False
        if not odb.odbLogout(self.handle, True):
            raise get_exception(self.handle)
        odb.odbFree(self.handle)
        del self.handle
        
    def commit(self):
        """Commit any pending transaction to the database.
        
        Note that some databases or ODBC drivers do not support transactions,
        and that this method will have no effect in those cases.
        """
        self._assert_connection_is_open()
        if not odb.odbCommit(self.handle):
            raise get_exception(self.handle)
        self.committed = True
    
    def rollback(self):
        """Restore database to the start of any pending transaction.
        
        Closing a connection without committing the changes first will
        cause an implicit rollback to be performed.
        Note that some databases or ODBC drivers do not support transactions,
        and that this method will have no effect in those cases.
        """
        self._assert_connection_is_open()
        if not odb.odbRollback(self.handle):
            raise get_exception(self.handle)
    
    def cursor(self):
        """Return a new Cursor Object using the connection.
        """
        self._assert_connection_is_open()
        cursorhandle = odb.odbAllocate(self.handle)
        if not cursorhandle:
            raise get_exception(self.handle)
        return Cursor(self)
    
    ############## Helper methods that are not part of the spec #############
    
    def _assert_connection_is_open(self):
        """Raise an error if the connection is no longer open.
        """
        if not self.open:
            raise InterfaceError('The connection has been closed.')
    
    def _get_driver(self):
        driver_buffer = c_char_p(' ' * 50)
        odb.odbGetAttrText(self.handle, ODB_ATTR_DRIVERNAME, driver_buffer, 50)
        return driver_buffer.value
    
    def _set_attributes(self):
        """Send attribute settings to the ODBTP server.
        
        Some of these may be driver dependent. This method is for internal
        use only and may change or disappear in future versions without notice.
        """
        if not odb.odbLoadDataTypes(self.handle):
            raise get_exception(self.handle)
        if not odb.odbSetAttrLong(self.handle, ODB_ATTR_DESCRIBEPARAMS, 0):
            raise get_exception(self.handle)
        if not odb.odbSetAttrLong(self.handle, ODB_ATTR_FULLCOLINFO, 1):
            raise get_exception(self.handle)
        if not odb.odbUseRowCache(self.handle, True, 0):
            raise get_exception(self.handle)
        
        if self.driver not in (ODB_DRIVER_FOXPRO, ODB_DRIVER_JET):
            # Enable transactions unless the driver is known to lack support.
            if not odb.odbSetAttrLong(
                    self.handle,
                    ODB_ATTR_TRANSACTIONS,
                    ODB_TXN_SERIALIZABLE
                    ):
                raise get_exception(self.handle)

class Cursor:
    """Object that manages the context of an operation.
    
    Cursors created from the same connection are not isolated. That is, any
    changes done to the database by a cursor are immediately visible by
    other cursors created from the same connection.
    """
    def __init__(self, connection):
        self.connection = connection
        self.handle = odb.odbAllocate(connection.handle)
        if not self.handle:
            raise get_exception(connection.handle)
        self.open = True
        self.arraysize = 1
        
        # The following are set to real values after execution.
        self.description = None
        self.prepared_operation = None
        self.input_sizes = ()
        self.rowcount = -1
    
    def __iter__(self):
        """Allow users to iterate over the cursor to fetch rows.
        """
        return self
    
    def __del__(self):
        """Clean up if the user doesn't close the cursor.
        """
        if self.open:
            self.close()
    
    def close(self):
        """Close the cursor.
        """
        if not odb.odbDropQry(self.handle):
            raise get_exception(self.handle)
        odb.odbFree(self.handle)
        self.open = False
    
    def callproc(self, procname, parameters=()):
        """Call a stored database procedure with the given name.
        
        The sequence of parameters must contain one entry for each argument
        that the procedure expects.
        """
        self.input_sizes = ()
        self.prepared_operation = None
        
        if not odb.odbPrepareProc(procname):
            self.connection.rollback()
            raise get_exception(self.handle)
        
        for value in parameters:
            db_api_type = get_db_api_type(self, value)
            # Called procedure parameters are automatically bound, so
            # we can set them without binding.
            db_api_type.set_parameter(value)
        
        if not odb.odbExecute(self.handle, None):
                self.connection.rollback()
                raise get_exception(self.handle)
        
        self._update_description()
        self.rowcount = odb.odbGetRowCount(self.handle)
        self.connection.committed = False
    
    def execute(self, operation, parameters=()):
        """Prepare and execute a database operation (query or command).
        
        Parameters may be provided as a sequence and will be bound to
        variables in the operation. Variables are specified using the
        qmark notation.
        """
        self.executemany(operation, (parameters,))
        return self
    
    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then
        execute it against all parameter sequences found in the
        sequence seq_of_parameters.
        """
        self._assert_cursor_is_open()
        
        try:
            total_cols = len(seq_of_parameters[0])
        except IndexError:
            raise InterfaceError('Parameters are required for .executemany()')
        
        if operation != self.prepared_operation:
            self._prepare_operation(operation, total_cols)
        
        for parameters in seq_of_parameters:
            if self.input_sizes:
                bound_parameters = map(None, self.input_sizes, parameters)
            else:
                bound_parameters = []
                for index, value in enumerate(parameters):
                    col_number = index + 1
                    db_api_type = get_db_api_type(self, value)
                    db_api_type.bind_to_column(self, col_number, total_cols)
                    bound_parameters.append((db_api_type, value))
            
            for db_api_type, value in bound_parameters:
                db_api_type.set_parameter(value)
            
            if not odb.odbExecute(self.handle, None):
                self.connection.rollback()
                raise get_exception(self.handle)
            
        self._update_description()
        self.rowcount = odb.odbGetRowCount(self.handle)
        self.connection.committed = False
        return self
    
    def fetchone(self):
        """Fetch the next row of a query result set
        
        Returns a single tuple, or None when no more data is available.
        """
        result = self.fetchmany(1)
        if result == []:
            return None
        else:
            return result[0]
    
    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result.
        
        Returns a list of tuples. An empty sequence is returned when no
        more rows are available.
        """
        self._assert_cursor_is_open()
        if size == None:
            size = self.arraysize
        
        rows = []
        while len(rows) < size:
            if not odb.odbFetchRow(self.handle):
                raise get_exception(self.handle)
            if odb.odbNoData(self.handle):
                break
            
            row = []
            for column in range(1, len(self.description)+1):
                if odb.odbColTruncated(self.handle, column):
                    msg = 'Column %d was truncated. Actual size is %d.' % (
                        column, odb.odbColActualLen(self.handle, column)
                        )
                    raise Warning(msg)
                
                data_address = odb.odbColData(self.handle, column)
                data_length = odb.odbColDataLen(self.handle, column)
                data_type = self.description[column - 1][1]
                row.append(get_data(data_address, data_type, data_length))
            rows.append(tuple(row))
    
        return rows
    
    def fetchall(self):
        """Fetch all remaining rows of a query result.
        
        Returns a list of tuples. Note that the cursor's arraysize
        attribute can affect the performance of this operation.
        """
        rows = []
        # We'll default to 20 rows at a time, but if the user has set
        # arraysize even larger, then we'll use that setting.
        size = max(20, self.arraysize)
        while True:
            new_rows = self.fetchmany(size)
            rows.extend(new_rows)
            if len(new_rows) < size:
                # All rows have been retrieved
                break
        
        return rows
    
    def next(self):
        """A method that, combined with __iter__(), makes the cursor iterable.
        
        This means that, after executing a query, one can iterate over the
        cursor (in a for loop, list comrehension, etc) to retrieve the
        rows of the result set one by one.
        """
        result = self.fetchmany(1)
        if result == []:
            raise StopIteration()
        else:
            return result[0]
    
    def nextset(self):
        """Switch to the next result set, if there is one.
        """
        self._assert_cursor_is_open()
        if not odb.odbFetchNextResult(self.handle):
            raise get_exception(self.handle)
    
    def setinputsizes(self, *sizes):
        """This can be used to predefine an operation's parameter information.
        
        sizes is a sequence of one item for each input parameter.
        The item should be a Type Object instance (examples: STRING(25),
        BINARY(250), NUMBER('float'), DATETIME('date'), or ROWID()) that
        corresponds to the input type and size that will be used.
        
        Unlike the description in the DB API spec, this implementation
        does not support None or an integer as items. Only Type Object
        instances are allowed.
        
        This method should be used before the first of a series of execute()
        calls that use the same query with similar parameters, or before an
        executemany(). It will give such queries a substantial performance
        boost.
        """
        if len(sizes) == 1 and isinstance(sizes[0], tuple):
            sizes = sizes[0]
        
        for item in sizes:
            if not isinstance(item, DbApiTypeObject):
                raise InterfaceError('Input sizes must use Type Objects.')
        
        self.input_sizes = tuple(sizes)
        
        # Any previous operation is no longer valid.
        self.prepared_operation = None
    
    def setoutputsize(self, size, column=None):
        """Ignored."""
        pass
    
    ############## Helper methods that are not part of the spec #############
    
    def _assert_cursor_is_open(self):
        """See if the cursor has been closed, and raise an exception if so.
        """
        if not self.open or not self.connection.open: 
            raise InterfaceError('Cursor or connection has been closed.')
    
    def _update_description(self):
        """Update the description attribute.
        
        This is for internal use only, to be run after an execute() or
        executemany().
        
        The description attribute is a sequence of 7-item sequences.
        Each of these sequences contains information describing one result
        column: (name, type_code, display_size, internal_size, precision,
        scale, null_ok). The first two items (name and type_code) are
        mandatory, the other five are optional and are set to None in this
        implementation of the standard.
        """
        odb.odbColName.restype = c_char_p
        num_columns = odb.odbGetTotalCols(self.handle)
        description = []
        for column in range(1, num_columns+1):
            col_description = (
                odb.odbColName(self.handle, column),
                get_odb_type(self.handle, column),
                None, None, None, None, None
                )
            description.append(col_description)
        self.description = description
    
    def _prepare_operation(self, operation, total_cols):
        """Prepare an operation with the ODBTP server.
        
        This also pre-binds any parameter info supplied by .setinputsizes()
        """
        if not odb.odbPrepare(self.handle, operation):
            self.connection.rollback()
            raise get_exception(self.handle)
        
        if self.prepared_operation != None:
            # Setting input sizes clears out an old prepared operation.
            # Since we have detected the old operation still exists,
            # that means the input sizes are invalid, and we should clear
            # them.
            self.input_sizes = ()
        
        self.prepared_operation = operation
        
        for index, db_api_type in enumerate(self.input_sizes):
            col_number = index + 1
            db_api_type.bind_to_column(self, col_number, total_cols)
