"""
This script will convert a metrics measurement file to a plot.
"""

import logging
import matplotlib.pyplot as plt
import os
import pandas
from lib import my_env

cfg = my_env.init_env("vdab", __file__)
logging.info("Start Application")

fd = cfg["Metrics"]["data_dir"]
plot_dir = cfg["Metrics"]["plot_dir"]
measurements = [fn for fn in os.listdir(fd) if fn[len(fn)-len(".csv"):] == ".csv"]
for fn in measurements:
    ft = fn[:-len(".csv")]
    ffp = os.path.join(fd, fn)
    df = pandas.read_csv(ffp)
    # Remove lines with t, value - this is added on every restart of the server
    cdf = df[df["value"] != "value"]
    val_int = pandas.to_numeric(cdf["value"])
    ts = pandas.to_datetime(cdf["t"], unit="s")
    plt.tick_params(axis="x", labelrotation=20)
    plt.plot(ts, val_int)
    title = ft.replace("_", " ").replace(".", " ").title()
    plt.title(title)
    plt.legend()
    pffn = os.path.join(plot_dir, "{ft}.png".format(ft=ft))
    plt.savefig(pffn)
    plt.gcf().clear()
