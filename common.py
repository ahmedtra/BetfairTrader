import threading
from configparser import ConfigParser
import os
from os import listdir
from os.path import isfile, join
import logging
import structlog
from collections import defaultdict
from structlog import configure
from structlog.processors import KeyValueRenderer
from structlog.stdlib import LoggerFactory

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def singleton(func):
    sentinel = {}
    result = [sentinel]
    result_lock = threading.Lock()

    def wrapper(*args,**kwargs):
        with result_lock:
            if result[0] is sentinel:
                result[0] = func(*args,**kwargs)
        return result[0]

    return wrapper


def process_singleton(func):
    sentinel = object()
    store = defaultdict(lambda: sentinel)
    lock = threading.Lock()

    def wrapper():
        pid = os.getpid()
        with lock:
            if store[pid] is sentinel:
                store[pid] = func()
        return store[pid]

    return wrapper



def get_config():
    config = ConfigParser()
    config_path = os.path.join(ROOT_DIR, 'config.md')
    config.read(config_path)
    return config


def get_data_path():
    conf = get_config()
    data_path = os.path.join(conf.get("files", "root"), conf.get("files", "histdata"))
    return data_path

def get_json_files_dirs():
    conf = get_config()
    data_path = conf.get("json_files", "root_path")
    data_completed = conf.get("json_files", "completed")
    return data_path, data_completed



def get_certif_path():
    conf = get_config()
    data_path = os.path.join(conf.get("files", "root"), conf.get("files", "certif"))
    certif_path = os.path.join(data_path, conf.get("auth", "certif"))
    return certif_path

def get_data_event_path():
    conf = get_config()
    data_path = os.path.join(conf.get("files", "root"), conf.get("files", "histdataEvent"))
    return data_path


def get_plot_path():
    conf = get_config()
    data_path = os.path.join(conf.get("files", "root"), conf.get("files", "plot"))
    return data_path

def list_all_files(path):
    onlyfiles = [os.path.join(path, f) for f in listdir(path) if isfile(join(path, f))]
    return onlyfiles

def initialize_logging(name):
    config = get_config()

    log_format = config.get('logging', 'log_format')
    log_datefmt = config.get('logging', 'log_datefmt')
    log_level = config.get('logging', 'log_lvl')
    log_dir=os.path.join(ROOT_DIR,'log')
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir,'{}.log'.format(name))

    configure(logger_factory=LoggerFactory(),
                processors=[
                    structlog.stdlib.filter_by_level,
                    KeyValueRenderer(sort_keys=True)
                ],
                cache_logger_on_first_use=True
              )

    logging.basicConfig(filename=log_file, level=logging.INFO, format=log_format, datefmt=log_datefmt)
    logging.root.setLevel(logging.getLevelName(log_level))

def safe_move(src, dst):

    if not os.path.isfile(dst):
        os.rename(src, dst)
        return dst
    else:
        return safe_move(src, dst + "_")