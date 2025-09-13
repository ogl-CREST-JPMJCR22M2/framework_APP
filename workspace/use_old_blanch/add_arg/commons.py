from iroha import Iroha, IrohaCrypto, IrohaGrpc
import SQLexecutor as SQLexe
from psycopg2 import sql
import time

iroha = Iroha('admin@test')
priv_key = 'f101537e319568c765b2cc89698325604991dca57b9716b58016b253506cab70'

###########################
## Get assembler from wsv ##
###########################

def get_assembler(partid, peer):  #peer:executing peer

    SQL = sql.SQL("""
            SELECT assembler FROM partinfo WHERE partid = {partid};
        """).format(
            partid = sql.Literal(partid)
        )
    return SQLexe.QUERYexecutor_wsv(SQL, peer)[0][0]


#############################
## Get cfp from offchainDB ##
#############################

def get_offchaindb_cfp(partid, peer):  #peer:target peer

    SQL = sql.SQL("""
            SELECT CFP FROM cfpval WHERE partid = {partid};
        """).format(
            partid = sql.Literal(partid)
        )
    return SQLexe.QUERYexecutor_off(SQL, peer)[0][0]


##################################
## Get totalcfp from offchainDB ##
##################################

def get_offchaindb_totalcfp(partid, peer):  #peer:target peer

    SQL = sql.SQL("""
            SELECT totalCFP FROM cfpval WHERE partid = {partid};
        """).format(
            partid = sql.Literal(partid)
        )
    return str(SQLexe.QUERYexecutor_off(SQL, peer)[0][0])


###########################
## Get Totalcfp from wsv ##
###########################

def get_wsv_totalcfp(partid, peer): #peer:target peer

    SQL = sql.SQL("""
            SELECT totalcfp FROM totalcfpval WHERE partid = {partid};
        """).format(
            partid = sql.Literal(partid)
        )
    return str(SQLexe.QUERYexecutor_wsv(SQL, peer)[0][0])


#######################
## UPDATE offchainDB ##
#######################

def update_data(partid, totalcfp, peer):  #peer:target peer
    SQL = sql.SQL("""
            UPDATE cfpval set totalcfp = {totalcfp} 
            WHERE partid = {partid} ;
        """).format(
            partid = sql.Literal(partid),
            totalcfp = sql.Literal(totalcfp)
        )

    SQLexe.COMMANDexecutor_off(SQL, peer)

#########d#######
## UPDATE wsv ##
################

def update_wsv(partid, totalcfp, peer):  #peer:target peer
    SQL = sql.SQL("""
            UPDATE totalcfpval set totalcfp = {totalcfp} 
            WHERE partid = {partid} ;
        """).format(
            partid = sql.Literal(partid),
            totalcfp = sql.Literal(totalcfp)
        )

    SQLexe.COMMANDexecutor_wsv(SQL, peer)


#######################
## Run iroha command ##
#######################

def IROHA_COMMANDexecutor(partid, cmd, peer): #peer:executing peer
    
    if peer[8:] == 'A':
        net = IrohaGrpc('192.168.32.2:50051')
    elif peer[8:] == 'B':
        net = IrohaGrpc('192.168.32.3:50051')
    else :
        net = IrohaGrpc('192.168.32.4:50051')

    tx = iroha.transaction(
        [iroha.command(
            cmd,
            account_id = 'admin@test',
            parts_id = partid
        )]
    )

    IrohaCrypto.sign_transaction(tx, priv_key)
    net.send_tx(tx)
    
    end_time = [time.time()]
    start_time = []

    for status in net.tx_status_stream(tx):
        #start_time.append(time.time())
        print(status)
        #end_time.append(time.time())
    
    #for i in range(4):
    #    print(start_time[i+1]-end_time[i])
    
    if status[0] == 'COMMITTED':
        totalcfp =  get_wsv_totalcfp(partid, peer)
        assembler = get_assembler(partid, peer)
        update_data(partid, totalcfp, assembler)
        return
    else:
        return
    

if __name__ == '__main__':

    partid = 'P00100'
    IROHA_COMMANDexecutor(partid, 'SetAccountDetail','postgresA')
    #IROHA_COMMANDexecutor(partid,'SetAccountDetail', 'postgresA')
    #IROHA_COMMANDexecutor(partid,'SubtractAssetQuantity', 'postgresA')
    