from betfair.betfair import Betfair
from common import get_config, get_certif_path

from structlog import get_logger

def authenticate():
    conf = get_config()
    username = conf.get("auth", "username")
    password = conf.get("auth", "pass")

    app_key = conf.get("auth", "appkey")
    certif = get_certif_path()

    get_logger().info("connecting to betfair api")

    client = Betfair(app_key, certif)
    client.login(username, password)

    get_logger().info("connected to the betfair api")

    return client