### partidを使って求める

import time
import hashlib
from decimal import *
from typing import Optional
import psycopg
from psycopg import sql
from collections import defaultdict
import mysql.connector

import commons as com

#==========#
# 必要設定  #
#==========#

assemblers = ["postgresA", "postgresB", "postgresC"]
ubuntus = ['ubuntuA', 'ubuntuB', 'ubuntuC']


#====================#
# ハッシュ部品木の生成  #
#====================#

def make_hash_parts_tree(assembler, assemblers, root_partid):

    conn: Optional[connection] = None
    try:
        conn = mysql.connector.connect(
                user='python_user',     # mysqlのユーザーID
                password='password',    # mysqlのrootユーザーのパスワード
                host=assembler,              # Ubuntuコンテナ名
                port=3306,              # mysqlのポート番号
                database='offchaindb'   # デフォルトで使用するDB
            )

        conn.autocommit = True

        with conn.cursor() as cur:

            ## 一時テーブルの構築
            cur.execute("""
                CREATE TEMPORARY TABLE target_tree (
                    partid CHARACTER varying(50),
                    parents_partid CHARACTER varying(50),
                    qty NUMERIC(30,0),
                    UNIQUE (partid, parents_partid)
                );
                
                CREATE TEMPORARY TABLE calc_cfp (
                    partid CHARACTER varying(50),
                    cfp DECIMAL, 
                    hash_cfp BINARY(32),
                    PRIMARY KEY (partid)
                );

                CREATE TEMPORARY TABLE hashvals(
                    partid CHARACTER varying(50),
                    parents_partid CHARACTER varying(50),
                    can_hashing boolean,
                    duplication boolean,
                    hash BINARY(32),
                    UNIQUE (partid, parents_partid)
                );

                CREATE INDEX idx_tree ON target_tree(partid);
                CREATE INDEX idx_cfp ON calc_cfp(partid);
                CREATE INDEX idx_hash ON hashvals(partid);
            """)

            ## cfpの算出
            # 他peerからのデータ収集
            co2_import = " UNION ALL \n".join(["SELECT * FROM dblink('host="+ p +" port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 'SELECT partid, co2 FROM cfpval') AS t1(partid CHARACTER varying(50), co2 DECIMAL)" 
            for p in peers ]) 
            
            # 部品木の抽出
            sql_1 = f"""
                INSERT INTO target_tree (partid, parents_partid, qty) 
                    WITH RECURSIVE all_tree AS (
                            SELECT partid, parents_partid, qty
                            FROM A_parts_tree

                            UNION ALL
                            
                            SELECT partid, parents_partid, qty
                            FROM B_parts_tree

                            UNION ALL
                            
                            SELECT partid, parents_partid, qty
                            FROM C_parts_tree
                        ),
                        get_tree AS ( 
                            SELECT partid, parents_partid, qty
                            FROM all_tree
                            WHERE partid = %s

                            UNION

                            SELECT a.partid, a.parents_partid, a.qty
                            FROM all_tree a, get_tree gt
                            WHERE a.parents_partid = gt.partid 
                        )
                        SELECT gt.partid, gt.parents_partid, qty
                        FROM get_tree gt;
                """
            cur.execute(sql_1, (root_partid, ))

            sql_2 = """
                -- cfp算出
                INSERT INTO calc_cfp (partid, cfp, hash_cfp) 
                WITH cfpvals AS(               
                    WITH RECURSIVE calc_qty(partid, root, quantity) AS (
                        SELECT DISTINCT
                            tt.partid,
                            tt.partid AS root,
                            1:: NUMERIC(100,0) AS quantity
                        FROM target_tree tt

                        UNION ALL

                        SELECT
                            tt.partid,          -- 子部品
                            cq.root,           -- スタート部品は固定
                            (cq.quantity * tt.qty):: NUMERIC(100,0) -- 親の個数 * 使用数（qty）
                        FROM calc_qty cq
                        JOIN target_tree tt ON tt.parents_partid = cq.partid
                    )
                    SELECT
                        cq.root AS partid,
                        ROUND(SUM(c.co2 * cq.quantity), 4) AS cfp
                    FROM calc_qty cq
                    JOIN co2vals c ON cq.partid = c.partid
                    GROUP BY cq.root
                    ORDER BY cq.root
                )
                SELECT partid, cfp, digest(cfp::text, 'sha256') AS hash_cfp
                FROM cfpvals;

                drop table co2vals;
            """
            cur.execute(sql_2)

            # 単品部品の処理
            sql_3 = f"""
                INSERT INTO hashvals (partid, parents_partid, can_hashing, duplication, hash)
                    WITH check_duplication AS (
                        SELECT partid, COUNT(partid) > 1 AS duplication
                        FROM target_tree
                        GROUP BY partid
                    )
                    SELECT 
                        tt.partid, tt.parents_partid,
                        -- ハッシュ化可能か
                        CASE 
                            WHEN EXISTS ( SELECT 1 FROM target_tree tt WHERE tt.parents_partid = calc_cfp.partid ) THEN False
                            ELSE True
                        END AS can_hashing,
                        -- 重複チェック
                        duplication,
                        hash_cfp AS hash
                    FROM calc_cfp, target_tree tt, check_duplication cd
                    WHERE calc_cfp.partid = tt.partid AND tt.partid = cd.partid;
            """
            cur.execute(sql_3)
            
            while True: 

                # 終了条件
                cur.execute("SELECT partid FROM hashvals WHERE can_hashing = False LIMIT 1;")
                row = cur.fetchone()

                if not row:
                    break

                sql_4 = """
                    WITH get_can_hashing AS (
                        SELECT 
                            parents_partid, 
                            bool_and(can_hashing)
                        FROM hashvals
                        GROUP BY parents_partid 
                        HAVING bool_and(can_hashing) = True
                    ), 

                    check_dup AS (
                        SELECT partid AS partid_org, gch.parents_partid AS partid,
                            CASE duplication 
                                WHEN True THEN digest( hash::text || gch.parents_partid::text, 'sha256')
                                ELSE hash
                            END AS hash_under_calc
                        FROM get_can_hashing gch, hashvals h
                        WHERE gch.parents_partid = h.parents_partid
                    ),

                    hashing AS (
                        SELECT 
                            partid,
                            array_agg(hash_under_calc) AS hash_list
                        FROM check_dup
                        GROUP BY partid 
                    )

                    UPDATE hashvals SET (can_hashing, hash) = (True, xor_sha256(hash_list || hash)) 
                    FROM hashing h
                    WHERE h.partid = hashvals.partid AND can_hashing = False;
                """ 
                cur.execute(sql_4)
                
            cur.execute( """
                WITH assembler_map AS (
                    SELECT partid, assembler
                    FROM A_assembler

                    UNION ALL

                    SELECT partid, assembler
                    FROM B_assembler

                    UNION ALL

                    SELECT partid, assembler
                    FROM C_assembler
                )
                SELECT DISTINCT calc_cfp.partid, assembler, cfp, encode(hash, 'hex') AS hashval 
                    FROM calc_cfp, hashvals, assembler_map ap
                    WHERE calc_cfp.partid = hashvals.partid AND calc_cfp.partid = ap.partid;
                """)
            data = cur.fetchall()

    finally:
        if conn:
            conn.close()
    
    return data


#====================#
# ハッシュ部品木の生成  #
#====================#

def tree_generation_process(root_partid):

    assembler = com.get_Assebler(root_partid)

    start = time.time()
    ## postgres処理
    result = make_hash_parts_tree(assembler, assemblers, root_partid)

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
        insert_val_dict[assembler].append((cfp, partid))

    # assemblerの辞書のkey
    assembler_unique = list(insert_val_dict.keys())

    # Irohaコマンドで書き込み
    com.IROHA_CMDexe(assembler, part_list, hash_list)

    # offchain-db (mysql) への書き込み
    for key in assembler_unique:

        upsert_sql = f"""
            UPDATE cfpval set cfp = %s
            WHERE partid = %s;
        """

        # DB接続
        conn = mysql.connector.connect(
            host="ubuntu" + key[-1],           # Peerのmysqlホスト
            user="python_user",
            password="password",
            database="offchaindb",
            port=3306
        )

        cur = conn.cursor()
        cur.executemany(upsert_sql, insert_val_dict[key])

        conn.commit()
        conn.close()
        cur.close()


# ======== MAIN ======== #

if __name__ == '__main__':

    root_partid = 'P0'

    start = time.time()

    #result = make_hash_parts_tree(assembler, assemblers, root_partid)
    #print(result)

    tree_generation_process(assembler, root_partid)
    
    t = time.time() - start
    print("time:", t)