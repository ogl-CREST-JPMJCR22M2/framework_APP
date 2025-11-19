# datasetをpostgresから削除するスクリプト

from typing import Optional
import psycopg

# DBのコネクションを返す
def createConnection(host):
    try:
        conn = psycopg.connect(
            dbname='iroha_default',
            user='postgres',
            password='mysecretpassword',
            port='5432',
            host=host
        )
    except psycopg.Error as e:
        print(f"error:{e}")
        return None
    
    return conn

    
if __name__ == '__main__':
    
    hosts = ['postgresA','postgresB', 'postgresC']

    sql = """
        delete from hash_parts_tree;
        delete from partrelationship;
        delete from partinfo;
    """

    conn: Optional[psycopg.Connection] = None
    
    for host in hosts:

        try:
            conn = createConnection(host)
            cur = conn.cursor()

            cur.execute(sql)
            conn.commit()

        except psycopg.Error as e:
            print(f"error: {e}")

        finally:
            cur.close()
            conn.close()
