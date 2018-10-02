"""
This script will call the neo4j2load scripts in sequence.
"""

# Allow lib to library import path.
import os
import logging
from lib import my_env
from lib.my_env import run_script

scripts = [
    "30_handle_clicks",
    "40_opleiding_competenties",
    "50_vacature_competenties",
    "60_burger_competenties",
    "90_load_neo4j"
    ]

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
(fp, filename) = os.path.split(__file__)
for script in scripts:
    logging.info("Run script: {s}.py".format(s=script))
    run_script(fp, "{s}.py".format(s=script))
logging.info("End Application")
