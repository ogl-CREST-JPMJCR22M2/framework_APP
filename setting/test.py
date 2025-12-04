for a in ['A', 'B', 'C']:
    exec('{} = {}'.format('assembler' + a, [['partid', 'assembler']]))
    exec('{} = {}'.format('parts_tree' + a, [['partid', 'parents_partid', 'qty']]))
    exec('{} = {}'.format('cfpval' + a, [['partid', 'cfp', 'co2']]))

print(assemblerA)