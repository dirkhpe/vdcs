"""
This procedure will rebuild the sqlite database
"""

import logging
from lib import my_env
from lib import sqlstore

cfg = my_env.init_env("vdab", __file__)
logging.info("Start application")
clicks = sqlstore.DirectConn(cfg)
clicks.rebuild()
logging.info("sqlite database clicks rebuild")
logging.info("End application")
