import hbspark as hs
import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql import Row

# import happybase as hb

spark = SparkSession.builder.appName("kxz167").master("local[1]").getOrCreate()

hs.connect(sys.argv[1], spark)
# print(hs.tables())

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
    print("Deleting existing table")
    hs.delete_table(new_table_name, True)

print("Creating new table: ",new_table_name)
new_table_families = {
    "cf1":dict(),
    "cf2":dict(),
    "cf3":dict()
}
new_table = hs.create_table(new_table_name, new_table_families)

print("Creating new row:")
test_dict = {
    "cf1:column1" : "value1",
    "cf1:column2" : "value2",
    "cf2:column1" : "value3",
    "cf2:column2" : "value4",
    "cf2:column3" : "value5"
}
print(test_dict)
hs_row = hs.hbase_row(test_dict)    #The spark ROW:
print(hs_row)
# Row(test_dict)
#Load the data into the table:
new_rowkey = "string_rowkey1"
new_table.put(new_rowkey,hs_row)
new_table.scan().show()

# Batch insertion of data:
print("Loading batch data")
batch_obj = new_table.batch()

# Add into batch
for x in range(2,10):
    loop_rowkey = "string_rowkey{}".format(x)
    loop_data = {
        'cf1:column1': "value1_rk" + str(x),
        'cf1:column2': "value2_rk" + str(x),
        'cf2:column1': "value3_rk" + str(x),
        'cf2:column2': "value4_rk" + str(x),
        'cf2:column3': "value5_rk" + str(x)
    }
    # loop_data = json.loads('{"cf1:column1": "value1_rk{}", "cf1:column2": "value2_rk{}", "cf2:column1": "value3_rk{}", "cf2:column2": "value4_rk{}", "cf2:column3": "value5_rk{}"}'.format(x,x,x,x,x))
    print(loop_rowkey, " : ",loop_data)
    print(hs.hbase_row(loop_data))
    batch_obj.put(loop_rowkey, hs.hbase_row(loop_data))

# Perform the operation.
batch_obj.send()
new_table.scan().show()