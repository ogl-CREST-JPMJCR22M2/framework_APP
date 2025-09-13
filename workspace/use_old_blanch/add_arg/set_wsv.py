import commons as common
import time

if __name__ == '__main__':
    start = time.time()

    #for i in range(1, 3334):
        
    #    part_id = f'P{i:05d}'
    #    common.IROHA_COMMANDexecutor(part_id, 'SetAccountDetail','postgresA')
    
    common.IROHA_COMMANDexecutor('P0', 'SetAccountDetail','postgresA')
    t = time.time() - start
    print(t)

    