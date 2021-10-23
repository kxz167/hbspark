from . import hb_session

def tables():
    global hb_session

    if(hb_session):
        return hb_session.connection().tables()