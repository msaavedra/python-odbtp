#!/usr/bin/env python

"""This is a simple testing script.

It is not currently suitable for general use, but with some modification
someone may find it worthwhile.
"""

import traceback
import sys
from decimal import Decimal

import odbtp as db

host = '192.168.1.1' # Insert the IP of a machine running the ODBTP server.
connect_string = 'DSN=SQLITETEST'
#connect_string = 'DSN=PGTEST'
#connect_string = 'DSN=ACCESSTEST'
#connect_string = 'BOGUS' # Fake DSN for testing

def get_traceback():
    info = sys.exc_info()
    tb = traceback.format_tb(info[2])
    tb = ''.join(tb)
    error = str(info[0])
    error = '%s%s: %s\r\n' % (tb, error, info[1])
    return error

try:
    print 'Connecting...'
    connection = db.connect(connect_string, server=host)
    print 'Driver:', connection.driver
    
    print 'Making cursor...'
    cursor = connection.cursor()
    
    #print 'Dropping test table...'
    #cursor.execute('DROP TABLE "OdbtpTest"')
    
    print 'Creating test table...'
    cmd = '''CREATE TABLE "OdbtpTest" (
    "TestField1" DATE,
    "TestField2" TIMESTAMP,
    "TestField3" TIME
    );'''
    cursor.execute(cmd)
    
    print 'Inserting Data...'
    cursor.setinputsizes(db.DATETIME('date'), db.DATETIME(), db.DATETIME('time'))
    cmd = '''INSERT INTO "OdbtpTest" 
        ("TestField1", "TestField2", "TestField3")
    VALUES (?, ?, ?)'''
    params1 = (
        db.Date(2008, 12, 31),
        db.Timestamp(2008, 2, 11, 16, 21, 26),
        db.Time(3, 15, 34)
        )
    params2 = (
        db.Date(2008, 12, 31),
        db.Timestamp(2005, 7, 11, 16, 21, 26),
        None
        )
    cursor.executemany(cmd, [params1, params2] * 10)
    print 'Rowcount:', cursor.rowcount
    """
    print 'Deleting all rows...'
    cursor.execute('DELETE FROM "OdbtpTest"')
    
    print 'Rolling back deletion...'
    connection.rollback()
    """
    print 'Executing select statement...'
    cursor.execute('SELECT * FROM "OdbtpTest"')
    print [(column[0]) for column in cursor.description]
    print 'Rowcount:', cursor.rowcount
    rows = cursor.fetchall()
    for row in rows:
        print row
    print 'Rowcount:', cursor.rowcount
    
    print 'Committing...'
    connection.commit()

finally:
    if 'cursor' in dir():
        print 'Closing cursor...'
        cursor.close()
    
    if 'connection' in dir():
        print 'Closing connection...'
        connection.close()
