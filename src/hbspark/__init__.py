# Happybase
import happybase as hb

# Global session management
from . import hb_session

# Initialize the connection:
def connect(hostname):
    hb_session.init(hb.Connection(hostname))

#Tables management:
from ._table import *