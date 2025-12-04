from typing import Optional
import mysql.connector

# DBのコネクションを返す
def createConnection(host):
    try:
        conn = mysql.connector.connect(
            host= 'ubuntu'+host,       # mysqlのサーバーアドレス
            user='root',      # mysqlのrootユーザーのパスワード
            password='password',
            port=3306,              # mysqlのポート番号
            database='offchaindb'       # デフォルトで使用するDB
        )
    except mysql.connector.Error as e:
        print(f"error:{e}")
        return None
    
    return conn

    
if __name__ == '__main__':

    conn = cur = None
    conn = createConnection('A')

    if conn is not None and conn.is_connected():
        cur = conn.cursor()
        try:
            # 既にユーザーが存在する場合のエラー回避のため CREATE USER IF NOT EXISTS を推奨
            # (MySQL 5.7.6以降で使用可能)
            cur.execute("CREATE USER IF NOT EXISTS 'python_user'@'%' IDENTIFIED BY 'password';")
            cur.execute("GRANT ALL PRIVILEGES ON offchaindb.* TO 'python_user'@'%';")
            cur.execute("FLUSH PRIVILEGES;")
            conn.commit()

        except mysql.connector.Error as e:
            print(f"SQL実行エラー: {e}")
        finally:
            cur.close()
            conn.close()
    else:
        print("Connection Failed")