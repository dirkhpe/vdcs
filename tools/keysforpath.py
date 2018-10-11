"""
This script will accept a urlpath start string. It will then find all logs in table clicks that start with the specified
string and find the distinct urlquery keywords.
"""

import argparse
import logging
import os
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
query = 'SELECT urlpath, urlquery FROM clicks WHERE urlpath like "{urlpath}%"'.format(urlpath=args.urlpath)
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
    # Get length of path
    path = parse.urlparse(rec["urlpath"]).path
    path_arr = path.split("/")
    k = "length_{cnt}".format(cnt=len(path_arr))
    try:
        keycnt[k] += 1
    except KeyError:
        keycnt[k] = 1
total = lc.end_loop()
resfn = os.path.join(cfg["Main"]["logdir"], "keycnt.csv")
resfile = open(resfn, "w")
resfile.write("Key;Count;Pct\n")
for k in sorted(keycnt, key=keycnt.get, reverse=True):
    resfile.write("{k};{cnt};{pct:.2f}\n".format(k=k, cnt=keycnt[k], pct=(keycnt[k]*100/total)))
resfile.close()
