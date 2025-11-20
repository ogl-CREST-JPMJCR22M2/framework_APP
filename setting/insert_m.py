# datasetをmariadbに挿入するスクリプト

from typing import Optional
import mariadb

# DBのコネクションを返す
def createConnection(host):
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

    
if __name__ == '__main__':

    palamater = '0/30000/3'  # フォルダ名に対応

    conn_B = conn_C = None
    cur_B = cur_C = None

    try:
        # ubuntuBへ
        conn_B = createConnection('ubuntuB')
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
        conn_C = createConnection('ubuntuC')
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
