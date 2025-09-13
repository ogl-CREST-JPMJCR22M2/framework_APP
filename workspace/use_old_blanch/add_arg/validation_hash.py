import commons as common
import time
import csv
import psutil
import SQLexecutor as SQLexe
from psycopg2 import sql

def Fine-grained_validation(partid, peer):

    assembler = common.get_assembler(partid, peer)

    offdb_value = common.get_offchaindb_totalcfp(partid, assembler) # Comparison destination : values in WSV

    common.IROHA_COMMANDexecutor(partid,'SetAccountDetail', peer)
    wsv_value = common.get_wsv_totalcfp(partid, peer) # Comparison source : values in offchainDB

    if float(wsv_value) != float(offdb_value):
        print("Validation Failed")
    else :
        print("Validation Successful")


def Coarse-grained_validation(partid, peer):

    assembler = common.get_assembler(partid, peer)

    offdb_totacfp = common.get_offchaindb_totalcfp(partid, assembler) # Comparison destination : values in WSV
    offdb_cfp = common.get_offchaindb_cfp(partid, assembler)

    if offdb_cfp < 0 or offdb_cfp > 1000:
        print("Faild Stateful Validation (cfp in offchainDB)")
        return 0

    SQL = sql.SQL("""
        WITH   
           general_table AS
            (   
                SELECT * FROM cfpval
                NATURAL RIGHT JOIN partinfo
            )
        SELECT parents_partid, SUM(totalcfp * duplicates) AS child_totalcfp
            FROM general_table
            WHERE parents_partid = {partid}
            GROUP BY parents_partid;
        """).format(
            partid = sql.Literal(partid)
        )
    child_totalCFP =  SQLexe.QUERYexecutor_wsv(SQL, peer)[0][1]

    if child_totalCFP < 0:
        print("Faild Stateful Validation (chilid totalCFP)")
        return 0
    
    new_totalcfp = child_totalCFP + offdb_cfp

    # if new_totalcfp > (2 ^ 256) / (10 ^ 10):
    #     print("Faild Stateful Validation (overflow new totalcfp)")
    #    return 0
        
    if float(new_totalcfp) != float(offdb_totacfp):
        print("Validation Failed")
    else :
        print("Validation Successful")


if __name__ == '__main__':

    time_data = []
    partid = 'P0'
    sumval = 0.0

    for n in range(10):
        start = time.time()
        Fine-grained_validation(partid,'postgresA')
        #Coarse-grained_validation(partid,'postgresA')

        t = time.time() - start
        sumval+= t
        #time_data.append([t])
    
    print(sumval/10.0)