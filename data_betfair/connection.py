
import threading

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
import logging

from common import get_config
from data.sql_wrapper.model import Competitions

__params = None

_session_lock = threading.Lock()
_session = None


def initialize_secdb():
    '''
    initialize secdbmodule
    :param params: a dic with following keys:
    secdb_url => connect string to the securities database
    parallelize => a boolean that indicate (only needed if you use future rolling feature)
    workers => number of processes for parallelization
    '''

    # invalidate previous session if it existed
    global _session
    global __params
    if _session is not None:
        assert isinstance(_session,scoped_session)
        _session.remove()

    _session = None

    params = dict(get_config().items("mysql"))

    assert isinstance(params, dict)
    # assert 'parallelize' in params  # excluded: not needed inside the package
    # assert 'workers' in params  # excluded: not needed inside the package

    __params = params

def create_sec_db_url():
    hostname = get_param("hostname")
    port = get_param("port")
    username = get_param("username")
    password = get_param("password")
    db = get_param("db")

    sql_url = "mysql://%(db_user)s:%(db_pwd)s@%(db_host)s:%(db_port)s/%(db_name)s" % \
                {"db_user": username, "db_pwd": password, "db_host": hostname, "db_port": port, "db_name": db}

    return sql_url

def get_param(key):
    return __params[key]

def flush_and_commit_to_secdb():
    try:
       get_session().flush()
       get_session().commit()
    except Exception as e:
        get_session().rollback()
        logging.error("Commit and flush to db is not successful. error_message = {}".format(e))

def get_session():
    '''
    :return: The one and only session to the db
    :rtype: session.Session
    '''
    global _session

    with _session_lock:
        try:
            #testing connections before handing it out for real usage.
            if _session:
                _session.query(Competitions).first()
        except:
            #Invalidating old, so new connection can be created in place of the broken one.
            logging.warn("Current session became invalid, dropping it")
            _session = None

        if not _session:
            logging.info("initializing new connection")
            db_url = create_sec_db_url()
            engine = create_engine(db_url, pool_recycle=1800)

            # for each dbapi connection in the pool make sure to set the writer argument.
            # if it is only set on the session level, connection pooling will switch to
            # another connection, where the writer has not been set!

            session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
            _session = scoped_session(session_factory)

    return _session

