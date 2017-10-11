import threading
from time import sleep

from betfair.betfair import Betfair

from common import get_config, get_certif_path, singleton

from structlog import get_logger


client = None
client_lock = threading.Lock()

def authenticate():
    conf = get_config()
    username = conf.get("auth", "username")
    password = conf.get("auth", "pass")

    app_key = conf.get("auth", "appkey")
    certif = get_certif_path()

    get_logger().info("connecting to betfair api")

    client = Betfair(app_key, certif, timeout=5)
    client.login(username, password)

    get_logger().info("connected to the betfair api")

    return client

@singleton
def get_client(reconnect = False):
    global client
    global client_lock
    with client_lock:
        if client is None or reconnect:
            client = authenticate()
    return client




