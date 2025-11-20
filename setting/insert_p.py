# datasetをpostgresに挿入するスクリプト

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

    palamater = '0/30000/3'  # フォルダ名に対応

    hosts = ['postgresA', 'postgresB', 'postgresC']

    filename = ['hash_part.csv', 'info.csv', 'relations.csv']
    tables = ['hash_parts_tree', 'partinfo', 'partrelationship']

    conn: Optional[psycopg.Connection] = None

    for host in hosts:

        try:
            conn = createConnection(host)
            cur = conn.cursor()

            for i in range(len(filename)):

                infile = f"/root/.github-private/hori/develop/example_use_in_db/dataset0724/{palamater}/{filename[i]}"

                copy_sql = f"""
                    COPY {tables[i]} FROM STDIN WITH (FORMAT csv, HEADER true)
                """

                with cur.copy(copy_sql) as copy:
                    with open(infile, "rb") as f:
                        copy.write(f.read())

            conn.commit()

        except psycopg.Error as e:
            print(f"error: {e}")

        finally:
            cur.close()
            conn.close()
