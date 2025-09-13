### 

import time
import hashlib
from decimal import *
from typing import Optional
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
from psycopg2._psycopg import connection, cursor
from collections import defaultdict

import SQLexecutor as SQLexe
import write_to_db as w


def update(peer, peers, target_partid, new_cfp):

    new_cfp =  Decimal(new_cfp).quantize(Decimal('0.0001'), ROUND_HALF_UP)

    conn: Optional[connection] = None
    try:
        dsn = {
            "dbname": "iroha_default",
            "user": "postgres",
            "password": "mysecretpassword",
            "port": "5432",
            "host": peer
        }
        conn = connect(**dsn)
        conn.autocommit = True

        with conn.cursor() as cur:

            

            ## 一時テーブルの構築
            cur.execute("""
                CREATE TEMP TABLE temp (
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    assembler CHARACTER varying(288),
                    cfp DECIMAL, 
                    co2 DECIMAL,
                    PRIMARY KEY (partid)
                );

                CREATE TEMP TABLE hash_v(
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    hash bytea,
                    PRIMARY KEY (partid)
                );

                CREATE INDEX idx_temp ON temp(partid);
                CREATE INDEX idx_hash ON hash_v(partid);
            """)
            
            ## cfpの算出
            # offchain-dbからcfpの算出
            co2_import = " UNION ALL \n".join(["SELECT * FROM dblink('host="+ p +" port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 'SELECT partid, cfp, co2 FROM cfpval') AS t1(partid CHARACTER varying(288), cfp DECIMAL, co2 DECIMAL)" 
            for p in peers ]) 
        
            sql_ = f"""
                INSERT INTO temp (partid, parents_partid, assembler, cfp, co2) 
                WITH cfpval AS (
                    {co2_import}
                ),
                get_path AS( 
                    WITH RECURSIVE calc(partid, parents_partid) AS 
                        ( 
                            SELECT partid, parents_partid
                            FROM partrelationship r
                            WHERE partid = %s

                            UNION ALL 

                            SELECT r.partid, r.parents_partid
                            FROM partrelationship r, calc 
                            WHERE r.partid = calc.parents_partid 
                        ) 
                        SELECT calc.partid, parents_partid, cfp, co2 
                        FROM  calc, cfpval
                        WHERE calc.partid = cfpval.partid
                )
                SELECT gp.partid, parents_partid, assembler, cfp, co2
                FROM get_path gp, partinfo pi
                WHERE gp.partid = pi.partid;

                SELECT co2 FROM temp WHERE partid = %s
            """
            cur.execute(sql_, (target_partid, target_partid,))
            pre_cfp = cur.fetchall()[0][0]
            sabun = Decimal(pre_cfp - new_cfp).quantize(Decimal('0.0001'), ROUND_HALF_UP)

            sql_1 = f"""
                WITH sabun AS (
                    SELECT partid, cfp - {sabun} AS new_cfp FROM temp
                )
                UPDATE temp SET cfp = new_cfp 
                FROM sabun
                WHERE sabun.partid = temp.partid;

                INSERT INTO hash_v (partid, parents_partid, hash)
                    SELECT pr.partid, pr.parents_partid, decode(hpt.hash, 'hex')
                    FROM partrelationship pr, hash_parts_tree hpt, temp
                    WHERE ( pr.partid = hpt.partid AND temp.partid = pr.parents_partid ) 
                     OR (pr.partid = 'P0' AND temp.partid = 'P0' AND hpt.partid = 'P0');
            """
            cur.execute(sql_1)

            now_target = target_partid

            # 葉ノードだったら次のノードへ
            cur.execute(f"SELECT r.parents_partid FROM partrelationship r, temp WHERE r.parents_partid = %s ", (now_target, ))
            row = cur.fetchone()
            
            if not row : 
                sql_2 = f"""
                    UPDATE hash_v SET hash = digest(cfp::text, 'sha256')
                    FROM temp
                    WHERE hash_v.partid = %s AND temp.partid = %s;
                    
                    SELECT parents_partid FROM temp WHERE partid = %s;
                """
                cur.execute(sql_2, (now_target, now_target, now_target, ))
                now_target = cur.fetchone()[0]
                           

            while True:

                cur.execute(f"SELECT parents_partid FROM temp WHERE partid = %s ", (now_target, ))
                row = cur.fetchone()
                
                sql_3 = f"""
                    WITH a AS (
                        SELECT parents_partid AS partid, array_agg(hash) AS hash_list
                        FROM hash_v
                        WHERE parents_partid = %s
                        GROUP BY parents_partid
                    ),
                    b AS (
                        SELECT a.partid, array_cat(ARRAY[digest(cfp::text, 'sha256')], hash_list) AS new_hash
                        FROM a, temp
                        WHERE a.partid = temp.partid
                    )
                    UPDATE hash_v SET hash = xor_sha256(new_hash)
                    FROM b
                    WHERE b.partid = hash_v.partid;
                """
                cur.execute(sql_3, (now_target, ))

                # ルートの先だったら終了
                if row == None:
                    break
                else:
                    now_target = row[0]

            sql_5 = """
                SELECT temp.partid, assembler, cfp, encode(hash, 'hex') as hashval 
                FROM temp, hash_v 
                WHERE temp.partid = hash_v.partid;
            """
            cur.execute(sql_5)
            data = cur.fetchall()
    finally:
        if conn:
            conn.close()
    
    return data


def make_merkltree(assembler, target_partid, new_cfp):

    peers = ["postgresA", "postgresB", "postgresC"]

    #start = time.time()
    ## postgres処理
    result = update(assembler, peers, target_partid, new_cfp)

    #print("ツリー構築",time.time()-start)
    #start = time.time()

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
    #start = time.time()

    # Irohaコマンドで書き込み
    SQLexe.IROHA_CMDexe(assembler, part_list, hash_list)

    #print("iroha実行",time.time()-start)
    #start = time.time()

    # offchain-dbへの書き込み
    for key in assembler_unique:

        upsert_sql = """
            UPDATE cfpval AS t
            SET
                partid = v.partid,
                cfp = v.cfp
            FROM (VALUES %s)
            AS v(partid, cfp)
            WHERE v.partid = t.partid;
        """

        # DB接続
        conn = connect(
            dbname = "offchaindb", 
            user = "postgres", 
            password = "mysecretpassword",
            host = key,
            port = 5432
        )

        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, insert_val_dict[key])



# ======== MAIN ======== #

if __name__ == '__main__':

    target_partid = 'P10'
    assembler = w.get_Assebler(target_partid)

    start = time.time()

    make_merkltree(assembler, target_partid, new_cfp)
    
    t = time.time() - start
    print("time:", t)
