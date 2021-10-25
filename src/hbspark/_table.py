# Get global session
from . import hb_session

#Modifications to tables.

# Returns a list of all of the sessions
def tables():
    return hb_session.query(hb_session.connection().tables)
    # global hb_session

    # if(hb_session.isInitialized()):
    #     return hb_session.connection().tables()
    
    # return None

# Select one table
def table(table_name):
    if(hb_session.isInitialized()):
        return hb_session.connection().table(table_name)
    
    return None

# def families(table_name):
#     global hb_session

#     if(hb_session.is)