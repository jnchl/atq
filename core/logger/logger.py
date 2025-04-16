import logging
import sys

# get the root logger
rootLogger = logging.getLogger()

# get a handler for console
console_handler = logging.StreamHandler(sys.stdout)

# create a formatter and attach it to console handler
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
console_handler.setFormatter(logFormatter)

# attach console handler to root logger
rootLogger.addHandler(console_handler)
rootLogger.setLevel(logging.INFO)

def get_logger(name: str):
    return rootLogger.getChild(suffix=name)