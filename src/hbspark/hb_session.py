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

def query(function):
    if(isInitialized):
        return function()
    
    return None