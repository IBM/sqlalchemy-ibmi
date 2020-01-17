""" This module specifies the specific dialect for the AS400 using the PyODBCConnector"""
# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008, 2016.                                |
# +--------------------------------------------------------------------------+
# | This module complies with SQLAlchemy 0.8 and is                          |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Jaimy Azle, Rahul Priyadarshi                                   |
# | Contributors: Mike Bayer                                                 |
# +--------------------------------------------------------------------------+

import urllib
from sqlalchemy import util
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from .base import DB2Dialect


class AS400DialectPyodbc(PyODBCConnector, DB2Dialect):
    """
    DB2 dialect for IBM i
    """
    supports_unicode_statements = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_decimal = True
    supports_char_length = True
    supports_native_decimal = False

    pyodbc_driver_name = "iSeries Access ODBC Driver"

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)

        keys = opts

        connect_args = {}
        for param in ('ansi', 'unicode_results', 'autocommit'):
            if param in keys:
                connect_args[param] = util.asbool(keys.pop(param))

        if 'odbc_connect' in keys:
            connectors = [urllib.parse.unquote_plus(keys.pop('odbc_connect'))]
        else:
            dsn_connection = 'dsn' in keys or \
                             ('host' in keys and 'database' not in keys)
            if dsn_connection:
                connectors = ['dsn=%s' % (keys.pop('host', '') or
                                          keys.pop('dsn', ''))]
            else:
                connectors = [
                    "DRIVER={%s}" %
                    keys.pop(
                        'driver',
                        self.pyodbc_driver_name),
                    'System=%s' %
                    keys.pop(
                        'host',
                        ''),
                    'DBQ=QGPL']
                connectors.append("PKG=QGPL/DEFAULT(IBM),2,0,1,0,512")
                db_name = keys.pop('database', '')
                if db_name:
                    connectors.append("DATABASE=%s" % db_name)

            user = keys.pop("user", None)
            if user:
                connectors.append("UID=%s" % user)
                connectors.append("PWD=%s" % keys.pop('password', ''))
            else:
                connectors.append("trusted_connection=yes")

            # if set to 'Yes', the ODBC layer will try to automagically convert
            # textual data from your database encoding to your client encoding
            # This should obviously be set to 'No' if you query a cp1253 encoded
            # database from a latin1 client...
            if 'odbc_autotranslate' in keys:
                connectors.append(
                    "AutoTranslate=%s" %
                    keys.pop("odbc_autotranslate"))

            connectors.extend(['%s=%s' % (k, v) for k, v in keys.items()])
        return [[";".join(connectors)], connect_args]


DIALECT = AS400DialectPyodbc
