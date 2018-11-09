import csv
from csv import Error
import json
import sys
import time
from lib import my_env
from lib import sqlstore
from lib.sqlstore import *
from os import listdir

cols = my_env.cols
urlpath_starters = my_env.urlpath_starters


# Define parsing as a function
def parse_line(fn, line_content):
    """
    This function will read every line in a log file and verify if the line needs to be kept.
    :param fn: Filename
    :param line_content: Line to be evaluated.
    :return:
    """
    try:
        result = []
        j_content = json.loads(line_content)
        if not isinstance(j_content, dict):
            # logging.critical("File: {fn} Line {lc} not translated to dictionary".format(fn=fn, lc=line_content))
            return False
        if '-cbt.' in j_content["vhost"]:
            return False
        for key in urlpath_starters:
            urlpath = j_content["urlpath"].lower()
            if key == urlpath[:len(key)]:
                break
        else:
            return False
        click_rec = {}
        for col in cols:
            try:
                result.append(j_content[col])
                click_rec[col] = j_content[col]
            except KeyError:
                logging.error("File: {fn} Field {c} not available in json line.".format(fn=fn, c=col))
                result.append("NotAvailable")
        # Add result to DB - but convert timestamp first to a timestamp accepted by sqlite
        click_rec["timestamp"] = click_rec["timestamp"][:19]
        click_rec["source"] = fn
        clicks = Click(**click_rec)
        sql_eng.add(clicks)
        return result
    except json.decoder.JSONDecodeError:
        # logging.critical("File: {fn} Invalid JSON line {lc}".format(fn=fn, lc=line_content))
        return False


cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
sql_eng = sqlstore.init_session(cfg["Main"]["db"])
parsed_dir = cfg["LogFiles"]["parsed_dir"]
raw_dir = cfg["LogFiles"]["raw_dir"]

files = []
filenames = []
for f in listdir(raw_dir):
    if "pa" == f[:2]:
        filenames.append(f)
        files.append(os.path.join(raw_dir, f))
files_total = len(filenames)
logging.info('{n} files imported.'.format(n=files_total))

# Defining the first filename
file_count = 0

cnt_tot_sum = {}
cnt_nok_sum = {}

for file in files:
    logging.info("Working on {f}".format(f=file))
    # Update new filename
    file_count += 1
    filename = "parsed_{fc}.csv".format(fc=file_count)
    start = time.time()
    cnt_tot = 0
    cnt_nok = 0
    output_list = []
    with open(file, errors='ignore') as f:
        for line in f:
            parsed_line = parse_line(file, line)
            cnt_tot += 1
            if parsed_line:
                output_list.append(parsed_line)
            else:
                cnt_nok += 1
    sql_eng.commit()

    # Parse all lines from the file
    # output_list.insert(0, cols)
    
    # Stats
    time_parsing = time.time()
    time_spent = round((time_parsing - start), 2)
    logging.info('time spent: {ts}'.format(ts=time_spent))
    logging.info("{fc} of {total} files extracted.".format(fc=file_count, total=files_total))
    
    with open(os.path.join(parsed_dir, filename), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        try:
            writer.writerows(output_list)
        except Error:
            logging.error("{f} resulted in empty file".format(f=file))
        except:
            e = sys.exc_info()[1]
            ec = sys.exc_info()[0]
            log_msg = "Error Class: %s, Message: %s"
            logging.critical(log_msg, ec, e)
            sys.exit()
            # Save result
    time_saving = time.time()
    time_spent = round((time_saving - start), 2)
    logging.info('time spent: {ts}'.format(ts=time_spent))
    logging.info("{fc} of {total} files saved.".format(fc=file_count, total=files_total))
    cnt_tot_sum[file] = cnt_tot
    cnt_nok_sum[file] = cnt_nok

sfn = os.path.join(cfg["Main"]["logdir"], "mp_parsing_stats.csv")
sfh = open(sfn, "w")
sfh.write("File;Total;NOK;Error Rate\n")
for file in cnt_tot_sum:
    sfh.write("{file};{tot};{nok};{pct}\n".format(file=file, tot=cnt_tot_sum[file], nok=cnt_nok_sum[file],
                                                  pct=cnt_nok_sum[file]/cnt_tot_sum[file]))
sfh.close()

logging.info("End Application")
