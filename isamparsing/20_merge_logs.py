"""
This is temporary script to remove header line from parsed files and merge info in one giant file.
"""
# import libraries
# import csv
import logging
import os
from lib import my_env
from os import listdir


cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")

# Set directories
parsed_dir = cfg["LogFiles"]["parsed_dir"]
merged_fn = cfg["LogFiles"]["merged_fn"]

# Get list of filenames in scandir, collect file full path names.
with open(merged_fn, "w",  encoding='utf-8') as fout:
    fi = my_env.LoopInfo("MergeFiles", 10)
    for f in listdir(parsed_dir):
        fi.info_loop()
        with open(os.path.join(parsed_dir, f), mode="r",  encoding='utf-8') as fin:
            for line in fin:
                fout.write(line)
    fi.end_loop()
logging.info("End Application")
