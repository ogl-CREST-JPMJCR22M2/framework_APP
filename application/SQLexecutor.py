from typing import Optional
from psycopg2 import connect, sql
from psycopg2._psycopg import connection, cursor
from iroha import Iroha, IrohaCrypto, IrohaGrpc, commands_pb2, endpoint_pb2, transaction_pb2

iroha = Iroha('admin@test')
priv_key = 'f101537e319568c765b2cc89698325604991dca57b9716b58016b253506cab70'


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


def QUERYexecutor_on(SQL, peer):
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

def COMMANDexecutor_on(SQL, peer):
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


def IROHA_CMDexe(peer, part_list, hash_list, cmd = "SubtractAssetQuantity"): #peer:executing peer
    
    if peer[8:] == 'A':
        net = IrohaGrpc('192.168.32.2:50051')
    elif peer[8:] == 'B':
        net = IrohaGrpc('192.168.32.3:50051')
    else :
        net = IrohaGrpc('192.168.32.4:50051')

    part_id = []
    hash_val = []

    cmd = commands_pb2.Command()
    cmd.subtract_asset_quantity.account_id = 'admin@test'
    cmd.subtract_asset_quantity.part_id.extend(part_list)
    cmd.subtract_asset_quantity.hash_val.extend(hash_list)

    # トランザクション作成
    tx = iroha.transaction([cmd])

    IrohaCrypto.sign_transaction(tx, priv_key)
    net.send_tx(tx)

    for status in net.tx_status_stream(tx):
        #print(status)
        pass

    