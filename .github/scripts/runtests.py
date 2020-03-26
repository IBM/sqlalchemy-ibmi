#!/usr/bin/env python

import os
import pyodbc
import pytest

host = os.environ['IBMI_HOSTNAME']
uid = os.environ['IBMI_USERNAME']
pwd = os.environ['IBMI_PASSWORD']

URI = "ibmi://{uid}:{pwd}@{host}/".format(
    host=host,
    uid=uid,
    pwd=pwd,
)

# Maybe can use a different schema in the future?
schema = uid

# allocate an object to serialize access to the test schemas
# without this, the tests will step on each other
# we can't use different schemas because some of the tests use the
# hard-coded test_schema schema
print("Connecting to the remote system", flush=True)
conn = pyodbc.connect(
    "Driver=IBM i Access ODBC Driver",
    system=host,
    user=uid,
    password=pwd,
)
cur = conn.cursor()

print("Attempting to lock the mutex", flush=True)
lock = "ALCOBJ OBJ(({schema}/CI_MUTEX *DTAARA *EXCL)) WAIT(30)".format(schema=schema)
while True:
    try:
        cur.execute("CALL QSYS2.QCMDEXC(?)", [lock])
        break
    except pyodbc.Error as e:
        if 'CPF1002' not in e.args[1]:
            raise e

print("Mutex locked, running tests", flush=True)
rc = pytest.main(["--dburi", URI])

print("Unlocking mutex", flush=True)
unlock = "DLCOBJ OBJ(({schema}/CI_MUTEX *DTAARA *EXCL))".format(schema=schema)
cur.execute("CALL QSYS2.QCMDEXC(?)", [unlock])

exit(rc)
