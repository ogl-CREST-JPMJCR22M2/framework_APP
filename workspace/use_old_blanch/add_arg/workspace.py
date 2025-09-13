import commons as common
import time
import csv
import psutil
import SQLexecutor as SQLexe
from psycopg2 import sql


def test():  #peer:executing peer

    SQL = sql.SQL("""
            WITH
            get_childpart AS
            (
                WITH RECURSIVE calcu(child_partid, parents_partid, duplicates) AS
                (
                    SELECT partrelationship.partid, partrelationship.parents_partid, duplicates
                    FROM partrelationship
                    WHERE partrelationship.parents_partid = 'P00001'
                    UNION ALL
                    SELECT partrelationship.partid, calcu.parents_partid, partrelationship.duplicates
                    FROM partrelationship, calcu
                    WHERE partrelationship.parents_partid = calcu.child_partid 
                )
                SELECT child_partid, duplicates
                FROM calcu
            ),
            import_table AS (
                SELECT * FROM dblink(
                    'host=postgresA port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 
                    'SELECT partid, cfp FROM cfpval') 
                    AS t1(partid CHARACTER varying(288), cfp DECIMAL)
                UNION ALL
                SELECT * FROM dblink(
                    'host=postgresB port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 
                    'SELECT partid, cfp FROM cfpval') 
                    AS t1(partid CHARACTER varying(288), cfp DECIMAL)
                UNION ALL
                SELECT * FROM dblink(
                    'host=postgresC port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 
                    'SELECT partid, cfp FROM cfpval') 
                    AS t1(partid CHARACTER varying(288), cfp DECIMAL)
            ),
            get_totalcfp AS
            (   
                SELECT sum(cfp * duplicates) as child_totalcfp
                FROM import_table INNER JOIN get_childpart ON get_childpart.child_partid = import_table.partid
            ), 
            new_quantity AS
             (
                 SELECT cfp, child_totalcfp + cfp as new_Totalcfp
                 FROM get_totalcfp, import_table
                 WHERE import_table.partid = 'P00001'
             ),
            checks AS -- error code and check result
            (
                -- source account exists
                SELECT 3 code, count(1) = 1 result
                FROM totalcfpval
                WHERE partid = 'P00001'

                -- check value of cfp
                UNION
                SELECT 4, cfp >= 0
                FROM new_quantity

		            -- check value of cfp
                UNION
                SELECT 5, cfp < 1000
                FROM new_quantity
                
                -- check value of sum_child_cfp
                UNION
                SELECT 6, child_totalcfp >= 0
                FROM get_totalcfp

                -- dest new_Totalcfp overflow
                UNION
                SELECT 7, new_Totalcfp < (2::decimal ^ 256) / (10::decimal ^ 10)
                FROM new_quantity
            ),
            create_hash AS
            (
                WITH parent_hash AS
                (
                    SELECT md5(new_Totalcfp::TEXT) AS P FROM new_quantity
                ),
                child_hash AS (
                    SELECT totalcfpval.partid, totalcfpval.hash AS C
                    FROM Partrelationship
                    JOIN totalcfpval ON Partrelationship.partid = totalcfpval.partid
                    WHERE Partrelationship.parents_partid = 'P00001'
                    ORDER BY totalcfpval.partid
                )
                SELECT (SELECT P FROM parent_hash) || STRING_AGG(C, '' ORDER BY partid) AS join_hash
                FROM child_hash
            )
	        UPDATE totalcfpval SET (hash, totalcfp) = 
                ((
                  SELECT md5(join_hash) FROM create_hash 
                ),
                (
                  SELECT new_Totalcfp FROM new_quantity 
                )) WHERE partid='P00001';
        """)
    return SQLexe.COMMANDexecutor_wsv(SQL, 'postgresA')

if __name__ == '__main__':

    sumval = 0.0

    for n in range(1):
        start = time.time()
        test()
        #simplified_validation(partsid,'postgresA')

        t = time.time() - start
        sumval+= t
        #time_data.append([t])
    
    #print(sumval/1.0)
