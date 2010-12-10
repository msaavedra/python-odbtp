# Copyright (c) 2010 Michael Saavedra

"""A subpackage that deals with data from python, odb and ctypes and handles
conversions between them.
"""

import time
import struct

from datetime import date, time, datetime
from ctypes import *
from decimal import Decimal

from odbtp.errors import *
from odbtp.constants import *

odb = cdll.LoadLibrary('libodbtp.so')

##################### Constructors from the DB API Spec ####################

def Date(year, month, day):
    return date(year, month, day)

def Time(hour, minute, second):
    return time(hour, minute, second)

def Timestamp(year, month, day, hour, minute, second):
    return datetime(year, month, day, hour, minute, second)

def DateFromTicks(ticks):
    return date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return datetime(*time.localtime(ticks)[:6])

class Binary(str):
    """An object that holds binary data.
    
    We use a class here rather than a factory function like the others
    because the builtin buffer type (which is the data type recommended
    for binary data by the DB API 2.0) is now deprecated.
    """
    
    def __repr__(self):
        return "Binary(%s)" % repr(str(self))

#################### Data Type objects from the API Spec ####################

class _DbApiType(type):
    """A metaclass allowing creation of DbApiTypeObjects
    
    This allows the DbApiTypeObject classes (not their instances) to be
    compared to the ODB types.
    """
    def __cmp__(cls, other):
        if other in cls.values:
            return 0
        if other < cls.values:
            return 1
        else:
            return -1

class _DbApiTypeObject(object):
    """An object that encapsulates data type information.
    
    This is just a base class, and is not part of the spec.
    """
    __metaclass__ = _DbApiType
    
    # This must be over-ridden in the sub-class definition.
    values = ()
    
    # The following may be over-ridden in a sub-class instance.
    odb_type = 0
    sql_type = 0
    size = 0
    max_size = 0
    precision = 0
    
    # The following will be assigned if an instance is bound as a parameter.
    cursor = None
    col_number = 0
    final = False
    bound = False
    odb_set_func = None
    
    def bind_to_column(self, cursor, col_number, total_cols):
        """Tell the ODBTP server to bind this data type to a parameter column.
        
        This gives the server explicit information about the data that will
        be supplied by a particular parameter in an operation.
        """
        self.cursor = cursor
        self.col_number = col_number
        self.final = (col_number == total_cols)
        self.bound = odb.odbBindParamEx(
            cursor.handle,
            col_number,
            ODB_PARAM_INOUT,
            self.odb_type,
            self.size,
            self.sql_type,
            self.max_size,
            self.precision,
            (col_number == total_cols)
            )
        if not self.bound:
            cursor.connection.rollback()
            raise get_exception(cursor.connection.handle)
    
    def set_parameter(self, value):
        """Set the parameter column to the given value.
        
        This data type object must have been bound to a particular paramter
        before calling this method. The value must also be consistent with
        the information supplied during binding.
        """
        if not self.bound:
            raise InterfaceError('You must bind a parameter before setting.')
        
        if value is None:
            ret_val = odb.odbSetParamNull(
                self.cursor.handle,
                self.col_number,
                self.final
                )
        else:
            ret_val = self.odb_set_func(
                self.cursor.handle,
                self.col_number,
                self.convert_to_c(value),
                self.final
                )
        
        if not ret_val:
            self.cursor.connection.rollback()
            raise get_exception(self.cursor.connection.handle)

class STRING(_DbApiTypeObject):
    values = (ODB_CHAR, ODB_WCHAR)
    
    def __init__(self, max_size):
        self.odb_type = ODB_CHAR
        self.sql_type = SQL_CHAR
        self.odb_set_func = odb.odbSetParamText
        self.max_size = max_size
        self.size = max_size

    def convert_to_c(self, value):
        return c_char_p(value)

class BINARY(_DbApiTypeObject):
    values = (ODB_BINARY,)
    
    def __init__(self, max_size):
        self.odb_type = ODB_CHAR
        self.sql_type = SQL_BINARY
        self.odb_set_func = odb.odbSetParamText
        self.max_size = max_size
        self.size = max_size
    
    def convert_to_c(self, value):
        return c_char_p(str(value))

class NUMBER(_DbApiTypeObject):
    values = (ODB_BIGINT, ODB_UBIGINT, ODB_BIT, ODB_DOUBLE, ODB_FLOAT,
                    ODB_INT, ODB_UINT, ODB_NUMERIC, ODB_REAL, 
                    ODB_SMALLINT, ODB_USMALLINT, ODB_TINYINT, ODB_UTINYINT)
    
    def __init__(self, sub_type='numeric'):
        self.sub_type = sub_type.lower()
        self.precision = 8
        
        if self.sub_type == 'int':
            self.odb_type = ODB_BIGINT
            self.sql_type = SQL_BIGINT
            self.odb_set_func = odb.odbSetParamLongLong
            self.convert_to_c = self._convert_int_to_c
        elif self.sub_type == 'float':
            self.odb_type = ODB_DOUBLE
            self.sql_type = SQL_DOUBLE
            self.odb_set_func = odb.odbSetParamDouble
            self.convert_to_c = self._convert_float_to_c
        elif self.sub_type == 'decimal':
            self.odb_type = ODB_CHAR
            self.sql_type = SQL_CHAR
            self.odb_set_func = odb.odbSetParamText
            self.convert_to_c = self._convert_decimal_to_c
        else:
            raise ProgrammingError('Illegal sub_type for NUMBER.')
    
    def _convert_int_to_c(self, value):
        return c_longlong(value)
    
    def _convert_float_to_c(self, value):
        return c_double(value)
    
    def _convert_decimal_to_c(self, value):
        return c_char_p(str(value))

class DATETIME(_DbApiTypeObject):
    values = (ODB_DATE, ODB_DATETIME, ODB_TIME)
    
    def __init__(self, sub_type='datetime'):
        self.sub_type = sub_type.lower()
        self.odb_type = ODB_CHAR
        self.sql_type = SQL_CHAR
        self.odb_set_func = odb.odbSetParamText
        
        if self.sub_type in ('datetime', 'timestamp'):
            self.max_size = 22
            self.size = 22
        elif  self.sub_type =='date':
            self.max_size = 10
            self.size = 10
        elif self.sub_type == 'time':
            self.max_size = 11
            self.size = 11
        else:
            raise ProgrammingError('Illegal sub_type for DATETIME.')
    
    def convert_to_c(self, value):
        return c_char_p(str(value))

class ROWID(_DbApiTypeObject):
    value = (ODB_GUID,)

###### Functions to aid conversion between Python, DB API and ODB data #####

def get_db_api_type(self, value):
    """Return a db api type instance appropriate for the given python value.
    """
    if value is None:
        return _DbApiTypeObject()
    elif isinstance(value, Binary):
        return BINARY(len(value))
    elif isinstance(value, str):
        return STRING(len(value))
    elif isinstance(value, datetime):
        return DATETIME()
    elif isinstance(value, date):
        return DATETIME('date')
    elif isinstance(value, time):
        return DATETIME('time')
    elif isinstance(value, int):
        return NUMBER('int')
    elif isinstance(value, Decimal):
        return NUMBER()
    elif isinstance(value, float):
        return NUMBER('float')
    else:
        raise DataError('Data type %s is not supported.' % str(type(value)))

def get_odb_type(handle, column):
    """Determine the proper data type for a column in a result set.
    
    ODBC drivers (and ODBTP itself?) are not always rigorous about specifying
    the proper data type, so this is a best effort to figure it out by
    examining both the SQL type and the ODB type.
    """
    sql_type = odb.odbColSqlType(handle, column)
    odb_type = odb.odbColDataType(handle, column)
    if sql_type == odb_type:
        return odb_type
    elif sql_type == 91 and odb_type == ODB_CHAR:
        return ODB_DATE
    elif sql_type == 92 and odb_type == ODB_CHAR:
        return ODB_TIME
    elif sql_type == -9 and odb_type == ODB_WCHAR:
        # XXX TODO: Change back to ODB_WCHAR when unicode works properly?
        return ODB_CHAR
    elif sql_type == 2 and odb_type == 1:
        return ODB_NUMERIC
    else:
        return odb_type

def get_data(address, data_type, length):
    """Get data as a python type from a particular area of memory.
    
    The address arg is the memory address of the data. The data_type is the
    odb type code of the data, and the length is the size of the data in bytes.
    """
    if address == 0 or address == None:
        return None
    try:
        return ODB_TO_PYTHON[data_type](address, length)
    except KeyError:
        raise DataError('Data type ID %d cannot be converted.' % data_type)

def _convert_binary(address, length):
    return Binary(string_at(address, length))

def _convert_int(address, length):
    return INT_SIZES[length].from_address(address).value

def _convert_uint(address, length):
    return UINT_SIZES[length].from_address(address).value

def _convert_float(address, length):
    return FLOAT_SIZES[length].from_address(address).value

def _convert_bit(address, length):
    return bool(UINT_SIZES[length].from_address(address).value)

def _convert_char(address, length):
    return string_at(address, length)

def _convert_date(address, length):
    args = string_at(address, length).split('-')
    return date(*[int(arg) for arg in args])

def _convert_time(address, length):
    args = string_at(address, length).split(':')
    return time(*[int(arg) for arg in args])

def _convert_datetime(address, length):
    fields = struct.unpack('H5hi', string_at(address, 16))
    return datetime(*fields)

def _convert_guid(address, length):
    # Not sure what this is for.
    return string_at(address, length)

def _convert_numeric(address, length):
    return Decimal(string_at(address, length))

def _convert_wchar(address, length):
    return wstring_at(address, length)

#### Code for building pre-calculated dictionaries of useful information ####

def _get_sizes(*data_types):
    """Return a mapping of ctypes and the byte size they use on this platform.
    """
    size_dict = {}
    for data_type in data_types:
        size = sizeof(data_type)
        if not size_dict.has_key(size):
            size_dict[size] = data_type
    return size_dict

INT_SIZES = _get_sizes(c_byte, c_short, c_int, c_long, c_longlong)
UINT_SIZES = _get_sizes(c_ubyte, c_ushort, c_uint, c_ulong, c_ulonglong)
FLOAT_SIZES = _get_sizes(c_float, c_double)

# Map odb data types and functions that convert them into python data types
ODB_TO_PYTHON = {
    ODB_BINARY: _convert_binary,
    ODB_BIGINT: _convert_int,
    ODB_UBIGINT: _convert_uint,
    ODB_BIT: _convert_bit,
    ODB_CHAR: _convert_char,
    ODB_DATE: _convert_date,
    ODB_DATETIME: _convert_datetime,
    ODB_DOUBLE: _convert_float,
    ODB_GUID: _convert_guid,
    ODB_INT: _convert_int,
    ODB_UINT: _convert_uint,
    ODB_NUMERIC: _convert_numeric,
    ODB_REAL: _convert_float,
    ODB_SMALLINT: _convert_int,
    ODB_USMALLINT: _convert_uint,
    ODB_TIME: _convert_time,
    ODB_TINYINT: _convert_int,
    ODB_UTINYINT: _convert_uint,
    ODB_WCHAR: _convert_wchar
    }
