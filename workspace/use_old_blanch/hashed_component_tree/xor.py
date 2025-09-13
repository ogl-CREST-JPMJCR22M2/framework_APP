
import hashlib

def xor_hash(strings):
    result = bytearray(32)  # SHA-256は32バイト（256ビット）
    for s in strings:
        h = hashlib.sha256(str(s).encode()).digest()
        for i in range(32):
            result[i] ^= h[i]  # XOR 合成
    return result.hex()

print(xor_hash(["67ea93d75b03a1dbe73977e6155fc4f951ac47f263915eac557376a99b704366", "5310b7b73026459f935aeb1020dd6303ee4414041d71edbd4b13ef8efce1a745"])) 
