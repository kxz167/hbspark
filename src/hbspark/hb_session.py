def init(connection):
    global hb_session
    hb_session = Session(connection)

# Manages open connection to happybase in order to make various queries.
class Session:
    def __init__(self, conn):
        self.conn = conn

    def connection(self):
        return self.conn