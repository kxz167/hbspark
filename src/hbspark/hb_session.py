# Initialize the hb_session
conn = None

def init(connection):
    global conn
    conn = connection

# Get the connection from the session
def connection():
    return conn

# If connection is initialized
def isInitialized():
    return conn != None

# Query the Happybase API by function name
def query(hb_function, *args, **kwargs):
    if(isInitialized()):
        return getattr(conn, hb_function)(*args, **kwargs)
    
    return None