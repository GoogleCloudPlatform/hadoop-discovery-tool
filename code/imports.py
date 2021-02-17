# Importing required libraries
import re
import datetime
import dateutil.parser
import os
import requests
import warnings
import subprocess
import sys
import math
import json
import os
import shutil
import xml.etree.ElementTree as ET
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pyhive import hive
from pprint import pprint
from fpdf import FPDF
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from getpass import getpass

# Defining default setting and date range for discovery report
sns.set(rc={"figure.figsize": (15, 5)})
pd.set_option("display.max_colwidth", 0)
pd.options.display.float_format = "{:,.2f}".format
warnings.filterwarnings("ignore")
sns.set(rc={"figure.figsize": (15, 5)})
date_range_start = datetime(2021, 1, 15, 0, 0, 0, 0)
date_range_end = datetime(2021, 2, 16, 17, 30, 0, 0)
start_date = date_range_start.strftime("%Y-%m-%dT%H:%M:%S")
end_date = date_range_end.strftime("%Y-%m-%dT%H:%M:%S")


def getInput(version):
    """Get input from user related to cloudera manger like Host Ip, Username, 
    Password and Cluster Name.

    Args:
        version (int): Cloudera distributed Hadoop version
    Returns:
        inputs (dict): Contains user input attributes

    """

    inputs = {}
    # inputs["version"] = version
    # inputs["cloudera_manager_host_ip"] = input("Enter Cloudera Manager Host IP: ")
    # inputs["cloudera_manager_username"] = input("Enter Cloudera Manager Username: ")
    # inputs["cloudera_manager_password"] = getpass(
    #     prompt="Enter Cloudera Manager Password: "
    # )
    # inputs["cluster_name"] = input("Enter Cluster Name: ")
    if version == 7:
        inputs["cloudera_manager_host_ip"] = "10.0.0.16"
        inputs["cluster_name"] = "MSTECH"
        inputs["cloudera_manager_username"] = "admin"
        inputs["cloudera_manager_password"] = "admin"
    elif version == 6:
        inputs["cloudera_manager_host_ip"] = "10.0.0.19"
        inputs["cluster_name"] = "QPTECH"
        inputs["cloudera_manager_username"] = "admin"
        inputs["cloudera_manager_password"] = "admin"
    elif version == 5:
        inputs["cloudera_manager_host_ip"] = "10.0.0.23"
        inputs["cluster_name"] = "cluster"
        inputs["cloudera_manager_username"] = "admin"
        inputs["cloudera_manager_password"] = "admin"
    return inputs


def getLogger():
    """Defining custom logger object with custom formatter and file handler.

    Returns:
        logger (obj): Custom logger object
    """

    logger = logging.getLogger("hadoop_discovery_tool")
    handler = logging.FileHandler("hadoop_discovery_tool_{}.log".format(datetime.now()))
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger
