#!/bin/bash
# docker nameに合わせて絶対に変更してください!!

export PGPASSWORD=mysecretpassword

datalink=("A" "B" "C")

for datalink in ${datalink[@]}; do
    psql -U postgres -d offchaindb -h "postgres-${datalink}" -c "\copy cfpval (partid, cfp, co2) from './dataset0724/0/30/1/off${datalink}.csv' with csv header;"
    psql -U postgres -d iroha_default -h "postgres-${datalink}" -c "\copy partinfo (partid, assembler) from './dataset0724/0/30/1/info.csv' with csv header;"
    psql -U postgres -d iroha_default -h "postgres-${datalink}" -c "\copy hash_parts_tree (partid, hash) from './dataset0724/0/30/1/hash_part.csv' with csv header;"
    psql -U postgres -d iroha_default -h "postgres-${datalink}" -c "\copy partrelationship (partid, parents_partid, qty) from './dataset0724/0/30/1/relations.csv' with csv header;"
done

exit $?
