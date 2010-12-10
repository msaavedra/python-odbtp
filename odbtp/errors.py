# Copyright (c) 2010 Michael Saavedra

"""Python DB API error handling.

This lays out the standard-defined exceptions, and contains a utility
function to convert ODBC and ODBTP error codes into the python exceptions.
"""


from ctypes import *

import exceptions

from odbtp.constants import *

odb = cdll.LoadLibrary('libodbtp.so')

class Warning(exceptions.StandardError):
    """Exception raised for important warnings, such as data truncations
    while inserting, etc.
    """
    pass

class Error(exceptions.StandardError):
    """Exception that is the base class of all other error exceptions. You
    can use this to catch all errors with one single 'except' statement.
    """
    pass

class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface,
    rather than the database itself.
    """
    pass

class DatabaseError(Error):
    """Exception raised for errors that are related to the database.
    """
    pass

class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range, etc.
    """
    pass

class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not found,
    a transaction could not be processed, a memory allocation error occurred
    during processing, etc.
    """
    pass

class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails.
    """
    pass

class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.
    """
    pass

class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not found or
    already exists, syntax error in the SQL statement, wrong number of
    parameters specified, etc.
    """
    pass

class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which
    is not supported by the database, e.g. requesting a .rollback() on a
    connection that does not support transaction or has transactions
    turned off.
    """
    pass

def get_exception(handle):
    """Return an appropriate instance of one of the DB API error classes.
    
    This is accomplished by examining the ODBTP and ODBC error codes.
    """
    odbtp_error = odb.odbGetError(handle)
    if odbtp_error == ODBTPERR_NONE:
        return Error('Unknown error.')
    elif ODBTP_ERRORS.has_key(odbtp_error):
        return ODBTP_ERRORS[odbtp_error]
    elif odbtp_error == ODBTPERR_SERVER:
        odb.odbGetErrorText.restype = c_char_p
        odbc_error = odb.odbGetErrorText(handle).strip()
        if odbc_error in ('', 'None'):
            return Error('Unknown Error.')
        
        odbc_error = odbc_error.replace('\r\n', '\n').replace('\r', '\n')
        error_parts = odbc_error.split('\n')[0].split(']')
        if len(error_parts) < 2:
            return Error(odbtp_error)
        error_code = error_parts[0][1:3].upper()
        error_subcode = error_parts[0][3:6].upper()
        
        if error_code == 'HY' and HY_ERROR_CODES.has_key(error_subcode):
            return HY_ERROR_CODES[error_subcode](odbc_error)
        elif ODBC_ERROR_CODES.has_key(error_code):
            return ODBC_ERROR_CODES[error_code](odbc_error)
        else:
            return Error(odbc_error)
    else:
        return Error('Unknown error.')

ODBTP_ERRORS = {
    ODBTPERR_MEMORY: InterfaceError('Memory error.'),
    ODBTPERR_HANDLE: InterfaceError('Connection or cursor handle error.'),
    ODBTPERR_CONNECT: InterfaceError('Connection error.'),
    ODBTPERR_READ: InterfaceError('Read error.'),
    ODBTPERR_SEND: InterfaceError('Send error.'),
    ODBTPERR_TIMEOUTCONN: InterfaceError('Connection timed out.'),
    ODBTPERR_TIMEOUTREAD: InterfaceError('Read timed out.'),
    ODBTPERR_TIMEOUTSEND: InterfaceError('Send timed out.'),
    ODBTPERR_CONNECTED: InterfaceError('Connection error.'), #???
    ODBTPERR_PROTOCOL: InterfaceError('Protocol error.'),
    ODBTPERR_RESPONSE: InterfaceError('Response error.'),
    ODBTPERR_MAXQUERYS: InterfaceError('Maximum queries exceeded.'),
    ODBTPERR_COLNUMBER: InterfaceError('Column number error.'),
    ODBTPERR_COLNAME: InterfaceError('Column name error.'),
    ODBTPERR_FETCHROW: InterfaceError('Error fetching row.'),
    ODBTPERR_NOTPREPPROC: InterfaceError('Not a prepared procedure.'),
    ODBTPERR_NOPARAMINFO: InterfaceError('Missing required parameter info.'),
    ODBTPERR_PARAMNUMBER: InterfaceError('Parameter number error.'),
    ODBTPERR_PARAMNAME: InterfaceError('Parameter name error.'),
    ODBTPERR_PARAMBIND: InterfaceError('Parameter binding error.'),
    ODBTPERR_PARAMGET: InterfaceError('Paramter retrieval error.'),
    ODBTPERR_ATTRTYPE: InterfaceError('Attribute type error.'),
    ODBTPERR_GETQUERY: InterfaceError('Query error.'), #???
    ODBTPERR_INTERFFILE: InterfaceError(), #???
    ODBTPERR_INTERFSYN: InterfaceError(), #???
    ODBTPERR_INTERFTYPE: InterfaceError(), #???
    ODBTPERR_CONNSTRLEN: InterfaceError('Connection string length error.'),
    ODBTPERR_NOSEEKCURSOR: InterfaceError('No seek cursor.'),
    ODBTPERR_SEEKROWPOS: InterfaceError('Seek row position error.'),
    ODBTPERR_DETACHED: InterfaceError('Connection has been detached.'), #???
    ODBTPERR_GETTYPEINFO: InterfaceError('Error getting type info.'),
    ODBTPERR_LOADTYPES: InterfaceError('Error loading types.'),
    ODBTPERR_NOREQUEST: InterfaceError('No request.'),
    ODBTPERR_FETCHEDROWS: InterfaceError('Error in fetched rows.'),
    ODBTPERR_DISCONNECTED: InterfaceError('Disconnected.'),  #???
    ODBTPERR_HOSTRESOLVE: InterfaceError('Cannot resolve host.'),
    }

ODBC_ERROR_CODES = {
    '01': Warning,
    '07': ProgrammingError,
    '08': InterfaceError,
    '21': ProgrammingError,
    '22': ProgrammingError,
    '23': IntegrityError,
    '24': InternalError,
    '25': InternalError,
    '28': DatabaseError,
    '34': InternalError,
    '3C': InternalError,
    '3D': ProgrammingError,
    '3F': ProgrammingError,
    '40': IntegrityError,
    '42': ProgrammingError,
    '44': DatabaseError,
    'HY': ProgrammingError,
    'IM': InterfaceError,
    }

# The HY class of error codes is very broad.
# Use this to look at how some subcodes map to python DB API errors
# If a subcode is not listed here, we should assume it is a ProgrammingError.
HY_ERROR_CODES = {
    '000': Error,
    '001': InterfaceError,
    '003': DataError,
    '004': DataError,
    '008': OperationalError,
    '014': InterfaceError,
    '018': OperationalError,
    'C00': NotSupportedError,
    'T00': OperationalError,
    'T01': OperationalError,
    }
