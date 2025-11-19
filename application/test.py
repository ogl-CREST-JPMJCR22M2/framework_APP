### partidを使って求める

import time
import hashlib
from decimal import *
from typing import Optional
import psycopg
from psycopg import sql
from collections import defaultdict
import mariadb

import SQLexecutor as SQLexe
import write_to_db as w

#==========#
# 必要設定  #
#==========#

p_peers = ["postgresA", "postgresB", "postgresC"]
m_peers = ['ubuntuA', 'ubuntuB', 'ubuntuC']

#==========================================#
# MariaDBからco2値を取得するためのfunction群   #
#==========================================#

# offchain-db(mariadb)からcfp値を取得
def get_co2_mariadb(m_peers: list[str]): # 引数は

    for host in m_peers:

        try:

            mconn = mariadb.connect(
                user='python_user',     # MariaDBのユーザーID
                password='password',    # MariaDBのrootユーザーのパスワード
                host=host,              # Ubuntuコンテナ名
                port=3306,              # MariaDBのポート番号
                database='offchaindb'   # デフォルトで使用するDB
            )

            mcur = mconn.cursor(buffered=False)  # ストリーミングモード

            mcur.execute("SELECT partid, co2 FROM cfpval;")

            for row in mcur:
                yield row

        except mariadb.Error as e:
            print(f"error:{e}")
        finally:
            mcur.close()

# copyコマンドでpostgresにロード
def load_into_postgres(pgcur):
    with pgcur.copy("COPY co2vals (partid, co2) FROM STDIN BINARY") as copy:
        for row in get_co2_mariadb(m_peers):
            copy.write_row(row)



#====================#
# ハッシュ部品木の生成  #
#====================#

def make_hash_parts_tree(p_peer, p_peers, root_partid):
    
    conn: Optional[psycopg.Connection] = None

    try:
        dsn = {
            "dbname": "iroha_default",
            "user": "postgres",
            "password": "mysecretpassword",
            "port": "5432",
            "host": p_peer
        }
        conn = psycopg.connect(**dsn)
        conn.autocommit = True

        with conn.cursor() as cur:

            ## 一時テーブルの構築
            cur.execute("""
                CREATE TEMP TABLE target_tree (
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    qty NUMERIC(100,0),
                    UNIQUE (partid, parents_partid)
                );

                CREATE TEMP TABLE co2vals (
                    partid CHARACTER varying(288),
                    co2 DECIMAL,
                    PRIMARY KEY (partid)
                );
                
                CREATE TEMP TABLE calc_cfp (
                    partid CHARACTER varying(288),
                    cfp DECIMAL, 
                    hash_cfp bytea,
                    PRIMARY KEY (partid)
                );

                CREATE TEMP TABLE hashvals(
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    can_hashing boolean,
                    duplication boolean,
                    hash bytea,
                    UNIQUE (partid, parents_partid)
                );

                CREATE INDEX idx_tree ON target_tree(partid);
                CREATE INDEX idx_co2 ON co2vals(partid);
                CREATE INDEX idx_cfp ON calc_cfp(partid);
                CREATE INDEX idx_hash ON hashvals(partid);
            """)
            
            # 部品木の抽出
            sql_1 = f"""
                INSERT INTO target_tree (partid, parents_partid, qty) 
                    WITH RECURSIVE get_tree(partid, parents_partid) AS 
                        ( 
                            SELECT partid, parents_partid, qty
                            FROM partrelationship
                            WHERE partid = %s

                            UNION

                            SELECT r.partid, r.parents_partid, r.qty
                            FROM partrelationship r, get_tree gt
                            WHERE r.parents_partid = gt.partid 
                        )
                        SELECT gt.partid, gt.parents_partid, qty
                        FROM get_tree gt;
                """
            cur.execute(sql_1, (root_partid, ))

            # mariadbからデータを収集
            load_into_postgres(cur)

            sql_2 = f"""
                -- cfp算出
                select * from co2vals;
            """
            cur.execute(sql_2)

            for row in cur:
                print(row)

    finally:
        if conn:
            conn.close()
    
    return data


#====================#
# ハッシュ部品木の生成  #
#====================#

def tree_generation_process(assembler, root_partid):

    start = time.time()
    ## postgres処理
    result = make_hash_parts_tree(assembler, p_peers, root_partid)

    #print("ツリー構築",time.time()-start)
    start = time.time()
    
    # polarsに変換
    part_list = []
    hash_list = []

    # assemblerごとの2次元リスト (insert_val)
    insert_val_dict = defaultdict(list)

    for partid, assembler, cfp, hashval in result:
        part_list.append(partid)
        hash_list.append(hashval)
        insert_val_dict[assembler].append((partid, cfp))

    # assemblerの辞書のkey
    assembler_unique = list(insert_val_dict.keys())

    #print("データの抽出",time.time()-start)
    start = time.time()

    # Irohaコマンドで書き込み
    SQLexe.IROHA_CMDexe(assembler, part_list, hash_list)

    #print("iroha実行",time.time()-start)
    start = time.time()

    # offchain-dbへの書き込み
    for key in assembler_unique:

        upsert_sql = """
            INSERT INTO cfpval (partid, cfp)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                cfp = VALUES(cfp);
        """

        # DB接続
        conn = mariadb.connect(
            host="ubuntu" + key,           # PeerのMariaDBホスト
            user="python_user",
            password="password",
            database="offchaindb",
            port=3306
        )

        cur.executemany(upsert_sql, insert_val_dict[key])

        conn.commit()
        conn.close()


# ======== MAIN ======== #

if __name__ == '__main__':

    root_partid = 'P0'
    assembler = w.get_Assebler(root_partid)

    start = time.time()

    result = make_hash_parts_tree(assembler, p_peers, root_partid)
    #print(result)

    #tree_generation_process(assembler, root_partid)
    
    t = time.time() - start
    print("time:", t)




