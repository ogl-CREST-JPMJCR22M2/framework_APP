# datasetをmariadbから削除するスクリプト

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
            database='offchaindb'       # デフォルトで使用するDB
        )
    except mariadb.Error as e:
        print(f"error:{e}")
        return None
    
    return conn

    
if __name__ == '__main__':

    conn_B = conn_C = None
    cur_B = cur_C = None

    sql = "delete from cfpval;"

    try:
        # ubuntuBへ
        conn_B = createConnection('ubuntuB')
        cur_B = conn_B.cursor()

        cur_B.execute(sql)
        conn_B.commit()

        # ubuntuCへ
        conn_C = createConnection('ubuntuC')
        cur_C = conn_C.cursor()

        cur_C.execute(sql)
        conn_C.commit()

    except mariadb.Error as e:
        print(f"error: {e}")

    finally:
        if cur_B: cur_B.close()
        if conn_B: conn_B.close()
        if cur_C: cur_C.close()
        if conn_C: conn_C.close()
