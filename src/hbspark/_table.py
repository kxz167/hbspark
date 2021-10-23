# Get global session
from . import hb_session

# Returns a list of all of the sessions
def tables():
    global hb_session

    if(hb_session.isInitialized()):
        return hb_session.connection().tables()