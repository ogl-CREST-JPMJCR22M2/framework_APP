### partidを使って求める

from sqlalchemy import create_engine
import polars as pl
import psycopg
from psycopg import sql


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


# Postgresのコネクションを確立
def createConnection(host):
    try:
        conn = psycopg.connect(
            dbname='iroha_default',
            user='postgres',
            password='mysecretpassword',
            port='5432',
            host=host
        )
    except mariadb.Error as e:
        print(f"error:{e}")
        return None
    
    return conn


### ASSEMBLERを取得

def get_Assebler(target_part):

    sql = f"SELECT assembler FROM partinfo WHERE partid = '{target_part}';"

    conn: Optional[psycopg.Connection] = None

    try:
        conn = createConnection('postgresA')
        cur = conn.cursor()

        cur.execute(sql)
        print(cur.fetchall()[0][0])

    except psycopg.Error as e:
        print(f"error: {e}")

    finally:
        cur.close()
        conn.close()

### 深さを取得

def get_hash():

    sql = """ 
        WITH RECURSIVE part_tree(partid, depth) AS (
            SELECT partid, 1
            FROM partrelationship
            WHERE parents_partid = 'P0'
            UNION ALL
            SELECT pr.partid, pt.depth + 1
            FROM partrelationship pr
            JOIN part_tree pt ON pr.parents_partid = pt.partid
        )
        SELECT MAX(depth) AS max_depth FROM part_tree;
    """
        
    conn: Optional[psycopg.Connection] = None

    try:
        conn = createConnection('postgresA')
        cur = conn.cursor()

        cur.execute(sql)
        print(cur.fetchall()[0][0])

    except psycopg.Error as e:
        print(f"error: {e}")

    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    get_Assebler('P0')