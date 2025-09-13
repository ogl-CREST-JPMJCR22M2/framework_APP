#!/bin/bash
# docker nameに合わせて絶対に変更してください!!

export PGPASSWORD=mysecretpassword

datalink=("A" "B" "C")

for datalink in ${datalink[@]}; do
    psql -U postgres -d offchaindb -h "postgres-${datalink}" -c "delete from cfpval;"
    psql -U postgres -d iroha_default -h "postgres-${datalink}" -c "delete from hash_parts_tree;"
    psql -U postgres -d iroha_default -h "postgres-${datalink}" -c "delete from partrelationship;"
    psql -U postgres -d iroha_default -h "postgres-${datalink}" -c "delete from partinfo;"
done

exit $?