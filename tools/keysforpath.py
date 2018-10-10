"""
This script will accept a urlpath start string. It will then find all logs in table clicks that start with the specified
string and find the distinct urlquery keywords.
"""

import argparse
import logging
from lib import my_env
from lib import sqlstore
from urllib import parse

# Configure command line arguments
parser = argparse.ArgumentParser(description="Get distinct keywords in urlquery for urlpath starter."
)
parser.add_argument('-p', '--urlpath', type=str, required=True,
                    help='Please provide urlpath start string for which distinct keywords are required.')
args = parser.parse_args()
cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
logging.info("Arguments: {a}".format(a=args))

cdb = sqlstore.DirectConn(cfg)
cdb.connect2db()

keycnt = {}
query = 'SELECT urlquery FROM clicks WHERE urlpath like "{urlpath}%"'.format(urlpath=args.urlpath)
res = cdb.get_query(query)
lc = my_env.LoopInfo("Query filters", 50)
for rec in res:
    lc.info_loop()
    queryparms = parse.parse_qsl(rec["urlquery"][1:])
    for k, v in queryparms:
        try:
            keycnt[k] += 1
        except KeyError:
            keycnt[k] = 1
total = lc.end_loop()
for k in sorted(keycnt, key=keycnt.get, reverse=True):
    print("{k}: {cnt} ({pct:.2f})%".format(k=k, cnt=keycnt[k], pct=(keycnt[k]*100/total)))
