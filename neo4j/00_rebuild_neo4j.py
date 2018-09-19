"""
This procedure will rebuild the sqlite database
"""

import logging
from lib import my_env
from lib import neostore

cfg = my_env.init_env("vdab", __file__)
logging.info("Start application")
ns = neostore.NeoStore(cfg, refresh="Yes")
logging.info("End application")
