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


def initialize_clean_rules(clean_file):
    """
    This function will initialize arrays for record cleaning.

    :param clean_file:

    :return: All arrays for cleaning are populated.
    """
    with open(clean_file, "r") as cfh:
        arrayhandle = []
        arrayname = ""
        for cfline in cfh:
            item = cfline.strip().lower()
            if item:
                logging.debug("Line: *{item}*".format(item=item))
                if item.startswith("[exc_"):
                    arrayname = item[5:-1]
                elif item.startswith("["):
                    arrayhandle = eval(item[1:-1])
                    arrayname = ""
                elif arrayname:
                    trail = item[:3]
                    arrayhandle = eval("{an}_{trail}".format(an=arrayname, trail=trail))
                    arrayhandle.append(item[4:])
                else:
                    arrayhandle.append(item)
    return


def clean_counter(cleanstr):
    """
    This method will keep track of the clean count. For every string in cleaning, the number of occurences is counted.

    :param cleanstr:

    :return:
    """
    try:
        cnt_clean[cleanstr] += 1
    except KeyError:
        cnt_clean[cleanstr] = 1
    return


def verify_urlpath(urlpath):
    """
    This method will check if urlpath is valid or if it requires to drop the record.

    :param urlpath:

    :return:
    """
    # First check on positive
    for val in urlpath_subexc_exc:
        if val in urlpath:
            clean_counter(val)
            return True
    # Then check for drop conditions - substring in string
    for val in (urlpath_subexc_rem + urlpath_substring):
        if val in urlpath:
            clean_counter(val)
            return False
    # Drop condition startswith
    for val in urlpath_startwith:
        if urlpath.startswith(val):
            clean_counter(val)
            return False
    # All checks done
    return True


def verify_urlquery(urlquery):
    """
    This method will check if urlquery is valid or if it requires to drop the record.

    :param urlquery:

    :return:
    """
    for val in urlquery_substring:
        if val in urlquery:
            clean_counter(val)
            return False
    # All checks done
    return True


def verify_user(user):
    for val in user_string:
        if val == user:
            clean_counter(val)
            return False
    # All checks done
    return True


def verify_clientip(clientip):
    for val in clientip_string:
        if val == clientip:
            clean_counter(val)
            return False
    # All checks done
    return True


def verify_vhost(vhost):
    for val in vhost_substring:
        if val in vhost:
            clean_counter(val)
            return False
    # All checks done - no failure
    return True


# Define parsing as a function
def parse_line(fn, line_content):
    try:
        result = []
        j_content = json.loads(line_content)
        if not isinstance(j_content, dict):
            logging.debug("File: {fn} Line {lc} not translated to dictionary".format(fn=fn, lc=line_content))
            return False
        keep_rec = verify_vhost(j_content['vhost'].lower())
        if keep_rec:
            keep_rec = verify_clientip(j_content['clientip'].lower())
        if keep_rec:
            keep_rec = verify_urlpath(j_content['urlpath'].lower())
        if keep_rec:
            keep_rec = verify_urlquery(j_content['urlquery'].lower())
        if keep_rec:
            keep_rec = verify_user(j_content["user"].lower())
        if not keep_rec:
            return False
        click_rec = {}
        for col in cols:
            try:
                result.append(j_content[col])
                click_rec[col] = j_content[col]
            except KeyError:
                logging.debug("File: {fn} Field {c} not available in json line.".format(fn=fn, c=col))
                result.append("NotAvailable")
        # Add result to DB - but convert timestamp first to a timestamp accepted by sqlite
        click_rec["timestamp"] = click_rec["timestamp"][:19]
        clicks = Click(**click_rec)
        sql_eng.add(clicks)
        return result
    except json.decoder.JSONDecodeError:
        logging.debug("File: {fn} Invalid JSON line {lc}".format(fn=fn, lc=line_content))
        return False


cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")
sql_eng = sqlstore.init_session(cfg["Main"]["db"])
parsed_dir = cfg["LogFiles"]["parsed_dir"]
raw_dir = cfg["LogFiles"]["raw_dir"]
# Initialize Clean Rules
urlpath_substring = []
urlpath_startwith = []
urlpath_subexc_rem = []
urlpath_subexc_exc = []
urlquery_substring = []
user_string = []
vhost_substring = []
clientip_string = []
initialize_clean_rules(cfg["Main"]["clean_rules"])

files = []
filenames = []
for f in listdir(raw_dir):
    filenames.append(f)
    files.append(os.path.join(raw_dir, f))
files_total = len(filenames)
logging.info('{n} files imported.'.format(n=files_total))

# Defining the first filename
file_count = 0

# pool = Pool()
cnt_tot_sum = {}
cnt_nok_sum = {}
cnt_clean = {}

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
    
    # save as .csv
    # Add columnnames as first list of lists
    # os.chdir(directory)
    # Be careful! csv.writer is binary and will break newlines. It may be required to specify newline='' in csv.writer
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

sfn = os.path.join(cfg["Main"]["logdir"], "mp_clean_stats.csv")
sfh = open(sfn, "w")
sfh.write("CleanStr;Total\n")
for k in cnt_clean:
    sfh.write("{k};{v}\n".format(k=k, v=cnt_clean[k]))
sfh.close()

logging.info("End Application")
