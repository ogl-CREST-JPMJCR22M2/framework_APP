# datasetをmariadbとpostgresに挿入するスクリプト

from typing import Optional
import mariadb
import psycopg
import sys 

# DBのコネクションを返す
def createConnection_m(host):
    try:
        conn = mariadb.connect(
            host=host,       # MariaDBのサーバーアドレス
            user='python_user',            # MariaDBのユーザーID
            password='password',    # MariaDBのrootユーザーのパスワード
            port=3306,              # MariaDBのポート番号
            database='offchaindb',       # デフォルトで使用するDB
            local_infile=1
        )
    except mariadb.Error as e:
        print(f"error:{e}")
        return None
    
    return conn


def execInsert_m(parameter):

    conn_B = conn_C = None
    cur_B = cur_C = None

    try:
        # ubuntuBへ
        conn_B = createConnection_m('ubuntuB')
        cur_B = conn_B.cursor()

        infile_B = f"/root/.github-private/hori/develop/example_use_in_db/dataset0724/{palamater}/offB.csv"

        sql_B = f"""
            LOAD DATA LOCAL INFILE '{infile_B}'
            INTO TABLE cfpval
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 ROWS;
        """

        cur_B.execute(sql_B)
        conn_B.commit()

        # ubuntuCへ
        conn_C = createConnection_m('ubuntuC')
        cur_C = conn_C.cursor()

        infile_C = f"/root/.github-private/hori/develop/example_use_in_db/dataset0724/{palamater}/offC.csv"

        sql_C = f"""
            LOAD DATA LOCAL INFILE '{infile_C}'
            INTO TABLE cfpval
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 ROWS;
        """

        cur_C.execute(sql_C)
        conn_C.commit()

    except mariadb.Error as e:
        print(f"error: {e}")

    finally:
        if cur_B: cur_B.close()
        if conn_B: conn_B.close()
        if cur_C: cur_C.close()
        if conn_C: conn_C.close()



# DBのコネクションを返す
def createConnection_p(host):
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


def execInsert_p(parameter):

    hosts = ['postgresA', 'postgresB', 'postgresC']

    filename = ['hash_part.csv', 'info.csv', 'relations.csv']
    tables = ['hash_parts_tree', 'partinfo', 'partrelationship']
    
    conn: Optional[psycopg.Connection] = None

    for host in hosts:

        try:
            conn = createConnection_p(host)
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


def execDelete_p():
    hosts = ['postgresA','postgresB', 'postgresC']

    sql = """
        delete from hash_parts_tree;
        delete from partrelationship;
        delete from partinfo;
    """

    conn: Optional[psycopg.Connection] = None
    
    for host in hosts:

        try:
            conn = createConnection_p(host)
            cur = conn.cursor()

            cur.execute(sql)
            conn.commit()

        except psycopg.Error as e:
            print(f"error: {e}")

        finally:
            cur.close()
            conn.close()


def execDelete_m():

    conn_B = conn_C = None
    cur_B = cur_C = None

    try:
        # ubuntuBへ
        conn_B = createConnection_m('ubuntuB')
        cur_B = conn_B.cursor()

        cur_B.execute("delete from cfpval;")
        conn_B.commit()

        # ubuntuCへ
        conn_C = createConnection_m('ubuntuC')
        cur_C = conn_C.cursor()

        cur_C.execute("delete from cfpval;")
        conn_C.commit()

    except mariadb.Error as e:
        print(f"error: {e}")

    finally:
        if cur_B: cur_B.close()
        if conn_B: conn_B.close()
        if cur_C: cur_C.close()
        if conn_C: conn_C.close()

    
if __name__ == '__main__':

    execDelete_m()
    execDelete_p()

    args = sys.argv

    palamater = args[1] if len(args) > 1 else '0/30000/3'  # フォルダ名に対応

    execInsert_m(palamater)
    execInsert_p(palamater)
    
