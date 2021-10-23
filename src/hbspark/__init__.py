import example
import happybase as hb

#Instantiation of the HB session:
hb_session = None

# Initialize the connection:
def connect(hostname):
    hb_session = Session(hb.Connection(hostname))

def tables(hostname):
    if(hb_session):
        return hb_session.connection().tables()

# Manages open connection to happybase in order to make various queries.
class Session:
    def __init__(self, conn):
        self.conn = conn

    def connection(self):
        return self.conn