from typing import Optional
import mariadb

# DBのコネクションを返す
def createConnection():
    try:
        conn = mariadb.connect(
            host='ubuntuB',       # MariaDBのサーバーアドレス
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
        CREATE USER 'python_user'@'%' IDENTIFIED BY 'password';
        GRANT ALL PRIVILEGES ON offchaindb.* TO 'python_user'@'%';
        FLUSH PRIVILEGES;
    '''

    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()

    except mariadb.Error as e:
        print(f"error:{e}")
    finally:
        cur.close()