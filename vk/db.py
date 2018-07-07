from contextlib import contextmanager
from pymongo import MongoClient

from db_sets import _uri, _authentication_database, _login, _passw

@contextmanager
def open_mongo_session(database, collection):
    connection = get_authorized_connection()
    try:
        yield connection[database][collection]
    finally:
        connection.close()

def get_authorized_connection(uri=None, authentication_database=None, login=None, passw=None):
    if not uri: uri = _uri
    if not authentication_database: authentication_database = _authentication_database
    if not login: login = _login
    if not passw: passw = _passw
    connection = MongoClient(uri);
    db = connection[authentication_database];
    db.authenticate(login, passw)
    return connection
