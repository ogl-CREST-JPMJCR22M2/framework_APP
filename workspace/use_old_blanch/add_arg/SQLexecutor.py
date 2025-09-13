from typing import Optional
from psycopg2 import connect, sql
from psycopg2._psycopg import connection, cursor

def QUERYexecutor_off(SQL, peer):
    conn: Optional[connection] = None
    try:
        dsn = {
            "dbname": "offchaindb",
            "user": "postgres",
            "password": "mysecretpassword",
            "port": "5432",
            "host": peer
        }

        conn = connect(**dsn)
        with conn.cursor() as cur:
            cur.execute(SQL)
            data = cur.fetchall()
            
    finally:
        if conn:
            conn.close()

    return data


def QUERYexecutor_wsv(SQL, peer):
    conn: Optional[connection] = None
    try:
        dsn = {
            "dbname": "iroha_default",
            "user": "postgres",
            "password": "mysecretpassword",
            "port": "5432",
            "host": peer
        }

        conn = connect(**dsn)
        with conn.cursor() as cur:
            cur.execute(SQL)
            data = cur.fetchall()
            
    finally:
        if conn:
            conn.close()

    return data


def COMMANDexecutor_off(SQL, peer):
    conn: Optional[connection] = None
    try:
        dsn = {
            "dbname": "offchaindb",
            "user": "postgres",
            "password": "mysecretpassword",
            "port": "5432",
            "host": peer
        }
        conn = connect(**dsn)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(SQL)

    finally:
        if conn:
            conn.close()

def COMMANDexecutor_wsv(SQL, peer):
    conn: Optional[connection] = None
    try:
        dsn = {
            "dbname": "iroha_default",
            "user": "postgres",
            "password": "mysecretpassword",
            "port": "5432",
            "host": peer
        }
        conn = connect(**dsn)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(SQL)

    finally:
        if conn:
            conn.close()
