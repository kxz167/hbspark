import hbspark as hs
import sys

from pyspark.sql import SparkSession
from pyspark.sql import Row

# import happybase as hb

spark = SparkSession.builder.appName("kxz167").master("local[1]").getOrCreate()

hs.connect(sys.argv[1], spark)
print(hs.tables())

# selected_table = hs.table('kxz167_test_table');
# print(hs.table('kxz167_test_table'))
# print(selected_table.families())
# for val in selected_table.scan():
#     print(val)
# selected_table.scan().show()
# print(selected_table.scan())
# print(hs.families('kxz167_test_table'))
# print("scanning")
# table = hs.table('kxz167_test_table')
# print(table.families());
# print(table.regions())
# table.scan(limit=2).show();
# SPARK CONNECTION:
# from pyspark.sql import SparkSession
# from pyspark.sql import Row
#
# spark = SparkSession.builder.appName("kxz167").master("local[1]").getOrCreate()
#
# df = spark.createDataFrame([
#     Row(column1='col1dat2', column2='col2dat2'),
#     Row(column1='col1dat3', column2='col2dat3')
# ])
#
# df.show()

# print(selected_table.row(1))

# print(hs.delete_table("kxz167_tt2p", disabled=True))


#SHOULD REALLY DO A JUPYTER NOTEBOOK:
#Attempt to create a table and input data:
new_table_name = 'kxz167-tt10'
if(hs.has_table(new_table_name)):
    # If a table exists:
    print("Deleted existing table")
    hs.delete_table(new_table_name, True)

print("Create new table")

new_table_families = {
    "cf1":dict(),
    "cf2":dict(),
    "cf3":dict()
}
new_table = hs.create_table('kxz167-tt10', new_table_families)

test_dict = {
    "cf1:column1" : "value1",
    "cf1:column2" : "value2",
    "cf2:column1" : "value3",
    "cf2:column2" : "value4",
    "cf2:column3" : "value5"
}

print(test_dict)
hs_row = hs.hbase_row(test_dict)
print(hs_row)
new_table.put("string_rowkey1",hs_row)
new_table.scan().show()