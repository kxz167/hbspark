import hbspark as hs
import sys

hs.connect(sys.argv[1])
print(hs.tables())

selected_table = hs.table('kxz167_test_table');
print(hs.table('kxz167_test_table'))
print(selected_table.families())
for val in selected_table.scan():
    print(val)
# print(selected_table.scan())
# print(hs.families('kxz167_test_table'))