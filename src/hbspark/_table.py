# Get global session
from . import hb_session
# from .Table import Table
# from .table import Table
#Modifications to tables.

# Returns a list of all of the sessions
def tables():
    return hb_session.query("tables")

# Select one table
def table(table_name):
    if(hb_session.isInitialized()):
        return Table(table_name)

    return None

# def families(table_name):
#     ret_table = table(table_name)
#     return ret_table.families() if ret_table is not None else ret_table

class Table:
    def __init__(self, name):
        self.table_ref = hb_session.query("table", name)

    # def query(self, hb_function, *args, **kwargs):
    #     return getattr(self.table_ref, hb_function)(*args, **kwargs)

    def families(self, *args, **kwargs):
        return self.table_ref.families(*args, **kwargs)

    def regions(self, *args, **kwargs):
        return self.table_ref.regions(*args, **kwargs)

    def row(self, row, *args, **kwargs):
        return self.table_ref.row(row, *args, **kwargs)

    def scan(self, *args, **kwargs):
        return self.table_ref.scan(*args, **kwargs) #This is a generator, can be used inside createDataFrame()