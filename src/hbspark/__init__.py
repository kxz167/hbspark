
# Import main api
import happybase as hb

#####################################################
#      Link to other distributed functionality      #
#####################################################
# Global session management
# Holds the spark session as well as open happybase connection
from . import hb_session

# Tables management functionality:
from ._table import *

# General utilitarian functions
from ._utils import *

# Function to initialize the connection:
# Built off hostname, and the spark_session.
def connect(hostname, spark_session):
    hb_session.init(hb.Connection(hostname), spark_session)