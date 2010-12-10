# Copyright (C) 2010 Michael Saavedra
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
# 
# Contact the author at:
#   mtsaavedra@gmail.com

"""Python bindings for the Open Database Transfer Protocol client library.

It is compliant with the Python Database API Specification v2.0 (see PEP 249
for details).

ODBTP is a TCP/IP protocol for connecting to Win32-based databases from any
platform. See http://http://odbtp.sourceforge.net/ for more information.
"""

__all__ = [
    'connection',
    'constants',
    'errors',
    'types',
    ]

apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

from odbtp.errors import Error, Warning, InterfaceError, DatabaseError, \
    InternalError, OperationalError, ProgrammingError, IntegrityError, \
    DataError, NotSupportedError

from odbtp.connection import connect

from odbtp.types import STRING, BINARY, NUMBER, DATETIME, ROWID, \
    Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, \
    Binary

