#!/bin/bash

export PGPASSWORD=mysecretpassword

datalink=("A" "B" "C")

for datalink in ${datalink[@]}; do
    psql -U postgres -d offchaindb -h "postgres-${datalink}" -c "drop database iroha_default;"
done

exit $?