import example
import happybase as hb

hb_session = None

def connect(hostname):
    hb_session = Session(hb.Connection(hostname))

def tables(hostname):
    if(hb_session):
        return hb_session.connection().tables()

class Session:
    def __init__(self, conn):
        self.conn = conn

    def connection():
        return self.conn