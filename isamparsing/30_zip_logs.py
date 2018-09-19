"""
This script will zip the logfile and the database in two different zip files..
"""
import logging
import os
import zipfile
from lib import my_env


cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")

# Set directories
# parsed_dir = cfg["LogFiles"]["parsed_dir"]
merged_fn = cfg["LogFiles"]["merged_fn"]
db = cfg["Main"]["db"]

# Zip merged file first
logging.info("Start zip {fn}".format(fn=merged_fn))
(fp, fn) = os.path.split(merged_fn)
zipfn = os.path.join(fp, "{fn}.zip".format(fn=fn.split(".")[0]))
zipf = zipfile.ZipFile(zipfn, 'w', zipfile.ZIP_DEFLATED)
zipf.write(merged_fn)
zipf.close()

logging.info("Start zip {fn}".format(fn=db))
(fp, fn) = os.path.split(db)
zipfn = os.path.join(fp, "{fn}.zip".format(fn=fn.split(".")[0]))
zipf = zipfile.ZipFile(zipfn, 'w', zipfile.ZIP_DEFLATED)
zipf.write(db)
zipf.close()
logging.info("End Application")
