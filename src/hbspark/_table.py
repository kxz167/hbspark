# _table.py
# Internal table methods
# Kris Zhao

###################
#     Imports     #
###################
# Get global session which holds setup information
from . import hb_session

# Grab all utility functions
from ._utils import *

###################
#     Methods     #
###################
# List out existing tables
# Returns list of strings of table names
def tables():
    return binlist_to_strlist(hb_session.query("tables"))

# Select a specific table based on string name
# Returns reference to a selected table as a _table.Table object
def table(table_name):
    if(hb_session.isInitialized()):
        return Table(table_name)
    return None

# Check if table name exists
# Return boolean of existence
def has_table(table_name):
    return table_name in tables()

# Create a new table in HBase
# Returns the _table.Table reference holder.
def create_table(name, families={}):
    # First create the new table
    hb_session._create_table(name, families)
    return Table(name) #Then select it by creating the object

# Delete a table:
# Returns None, deletes the corresponding table.
def delete_table(name, disabled=False):
    hb_session._delete_table(name, disabled)

# Get a table (or create an empty one)
# Returns _table.Table object
def get_or_create_table(table_name, families):
    if(has_table(table_name)):
        return table(table_name)
    else:
        return hb_session._create_table(table_name, families)

# Get the column families inside a table:
# Returns Dict for each column family.
def families(table_name):
    ret_table = table(table_name)
    return ret_table.families() if ret_table is not None else ret_table

###################
#     Objects     #
###################
class Table:
    def __init__(self, name):
        self.table_ref = hb_session.query("table", name)

    # Informational:
    def families(self, *args, **kwargs):
        return bindict_to_strdict(self.table_ref.families(*args, **kwargs))

    def regions(self, *args, **kwargs):
        return bindict_to_strdict(self.table_ref.regions(*args, **kwargs))

    # Specific data retrieval
    def row(self, row, *args, **kwargs):
        return hb_session.create_row(**bindict_to_strdict(self.table_ref.row(str(row), *args, **kwargs)))

    #TODO Convert to pyspark
    def cells(self, row, column, *args, **kwargs):
        return self.table_ref.cells(row, column, *args, **kwargs)

    # Table modification:
    def put(self, row, data, *args, **kwargs):
        dataDict = data.asDict(True) #Must be structured as "columnfamily:column"
        # print(dataDict)
        return self.table_ref.put(row, dataDict, *args, **kwargs)

    # Deletes the row at the given rowkey, can specify columns.
    def delete(self, rowkey, *args, **kwargs):
        return self.table_ref.delete(rowkey, *args, **kwargs)

    #For bulk puts / deletes of rows.
    def batch(self, *args, **kwargs):
        return Batch(self.table_ref.batch(*args, **kwargs))

    # *COUNTERS?*

    # Creates a new dataframe from the specified scan
    # Appends rowkey to the front of the dictionary
    def scan(self, *args, **kwargs):
        data_generator = self.table_ref.scan(*args, **kwargs)
        # for index, row in data_generator:
        #     print(row)
        return hb_session.create_data_frame([
            dict(bindict_to_strdict(column_data),rowkey=row_index.decode("utf-8")) for row_index, column_data in self.table_ref.scan(*args, **kwargs)
        ])

class Batch:
    def __init__(self, hb_batch):
        self.hb_batch = hb_batch

    # Submit the batch process
    def send(self):
        self.hb_batch.send()

    # Put the Pyspark row into the database
    def put(self, row, data, wal=None):
        print(data)
        self.hb_batch.put(row, data.asDict(True), wal)

    def delete(self, row, column=None, wal=None):
        self.hb_batch.put(row, column, wal)

