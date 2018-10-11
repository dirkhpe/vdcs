"""
This script will accept a urlpath start string, a minimum length and a maximum length.
It will find the three examples of urlpath for every length. The result will be printed to a text file.
"""

import argparse
import logging
import os
import sys
from lib import my_env
from lib import sqlstore
from urllib import parse

# Configure command line arguments
parser = argparse.ArgumentParser(description="Get for urlpath starter the different formats of the urlpath."
)
parser.add_argument('-p', '--urlpath', type=str, required=True,
                    help='Please provide urlpath start string for which records will be required')
parser.add_argument('-l', '--lowlength', type=int ,required=True,
                    help='What is the minimum number of parts in the path?')
parser.add_argument('-u', '--upperlength', type=int, required=True,
                    help='What is the maximum number of parts in the path?')
args = parser.parse_args()
cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
logging.info("Arguments: {a}".format(a=args))

pathcnt = {}
resarr = []

low = args.lowlength
high = args.upperlength
if low > high:
    sys.exit()
for cnt in range(low, high+1):
    pathcnt["len{cnt}".format(cnt=cnt)] = 0

cdb = sqlstore.DirectConn(cfg)
cdb.connect2db()

query = 'SELECT urlpath, urlquery FROM clicks WHERE urlpath like "{urlpath}%"'.format(urlpath=args.urlpath)
res = cdb.get_query(query)
lc = my_env.LoopInfo("Query filters", 50)
for rec in res:
    lc.info_loop()
    # Get length of path
    path = parse.urlparse(rec["urlpath"]).path
    path_arr = path.split("/")
    k = "len{cnt}".format(cnt=len(path_arr))
    try:
        pathcnt[k] += 1
        resarr.append(rec["urlpath"])
        if pathcnt[k] >= 3:
            pathcnt.pop(k)
    except KeyError:
        # This is a length we don't care about (anymore)
        pass
    if len(pathcnt) == 0:
        break
total = lc.end_loop()
resfn = os.path.join(cfg["Main"]["logdir"], "urlpath.txt")
resfile = open(resfn, "w")
resfile.write("\n".join(resarr))
resfile.close()
