"""
This script will convert a metrics measurement file to a plot.
"""

import argparse
import datetime
import logging
import matplotlib.pyplot as plt
import os
import pandas
import pytz
from lib import my_env

parser = argparse.ArgumentParser(
    description="Convert Neo4J Measurement data to plots."
)
parser.add_argument('-s', '--startTime', required=False,
                    type=lambda d: datetime.datetime.strptime(d, "%d/%m/%Y %H:%M"),
                    help='Optional - add start time for plot interval. Format: "dd/mm/yyyy hh:mm" (remember quotes')
parser.add_argument('-d', '--duration', type=int, required=False,
                    help='Optional - add duration (hours) for plot interval.')
args = parser.parse_args()

starttime = args.startTime
duration = args.duration
if starttime:
    # Convert starttime to UTC, keep naive date time.
    # Set timezone
    eu = pytz.timezone('Europe/Brussels')
    # Localize starttime
    local_starttime = eu.localize(starttime)
    # Get timezone delta
    tzd = int(local_starttime.strftime("%z")[2:3])
    starttime = starttime - datetime.timedelta(hours=tzd)

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")

fd = cfg["Metrics"]["data_dir"]
plot_dir = cfg["Metrics"]["plot_dir"]
measurements = [fn for fn in os.listdir(fd) if fn[len(fn)-len(".csv"):] == ".csv"]
for fn in measurements:
    ft = fn[:-len(".csv")]
    logging.info("Working on {fn}".format(fn=fn))
    ffp = os.path.join(fd, fn)
    df = pandas.read_csv(ffp)
    df = df[df["value"] != "value"]
    # Convert objects to datetime and int
    df = df.assign(t=pandas.to_datetime(df.t, unit="s"),
                   value=pandas.to_numeric(df.value))
    if starttime:
        df = df[df["t"] > starttime]
        if duration:
            endtime = starttime + datetime.timedelta(hours=duration)
            df = df[df["t"] <= endtime]
    plt.tick_params(axis="x", labelrotation=20)
    plt.plot(df.t, df.value)
    title = ft.replace("_", " ").replace(".", " ").title()
    plt.title("{title} (UTC)".format(title=title))
    pffn = os.path.join(plot_dir, "{ft}.png".format(ft=ft))
    plt.savefig(pffn)
    plt.gcf().clear()
