# import example
import happybase as hb
from . import hb_session

#Instantiation of the HB session:
# hb_session = None

# Initialize the connection:
def connect(hostname):
    hb_session.init(hb.Connection(hostname))
    # hb_session = Session(hb.Connection(hostname))

