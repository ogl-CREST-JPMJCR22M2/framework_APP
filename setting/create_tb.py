from typing import Optional
import mariadb

# DBのコネクションを返す
def createConnection():
    try:
        conn = mariadb.connect(
            host='ubuntuC',       # MariaDBのサーバーアドレス
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

    conn = createConnection()
    sql = '''
        CREATE TABLE IF NOT EXISTS cfpval(
            partid CHARACTER varying(288) PRIMARY KEY,
            cfp DECIMAL(18, 4) NOT NULL ,
            co2 DECIMAL(18, 4) NOT NULL
            );
    '''

    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()

    except mariadb.Error as e:
        print(f"error:{e}")
    finally:
        cur.close()