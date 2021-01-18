import logging

logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y.%m.%d %H:%M:%S %Z",
    format="%(asctime)s - %(levelname)8s - %(name)s - %(message)s",
)


class Config(object):
    def __init__(self, host, port, user, password, vhost, queue, all_queue):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.queue = queue
        self.all = all_queue
