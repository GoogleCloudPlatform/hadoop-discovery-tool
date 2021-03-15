# -------------------------------------------------------------------------------
# This module imports all the python packages required throughout the code and
# also initializes global variables which will be used throughout the code as
# arguments.
# ------------------------------------------------------------------------------

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
import glob
from pprint import pprint
from fpdf import FPDF
from pandas.errors import EmptyDataError
from os import path
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from getpass import getpass
from sqlalchemy import create_engine
from tqdm import tqdm
from xml.etree.ElementTree import XML, fromstring
from time import sleep

# Defining default setting and date range for assessment report
sns.set(rc={"figure.figsize": (15, 5)})
pd.set_option("display.max_colwidth", 0)
pd.options.display.float_format = "{:,.2f}".format
warnings.filterwarnings("ignore")
sns.set(rc={"figure.figsize": (15, 5)})
# date_range_start = datetime(2021, 2, 20, 0, 0, 0, 0)
# date_range_end = datetime(2021, 2, 24, 15, 30, 0, 0)
# start_date = date_range_start.strftime("%Y-%m-%dT%H:%M:%S")
# end_date = date_range_end.strftime("%Y-%m-%dT%H:%M:%S")


def checkSSL():
    """Check whether SSL is enabled or not

    Returns:
        ssl (bool): SSL flag
    """

    ssl = True
    xml_data = os.popen("cat /etc/hadoop/conf/core-site.xml").read()
    root = ET.fromstring(xml_data)
    for val in root.findall("property"):
        name = val.find("name").text
        if "hadoop.ssl.enabled" not in name:
            root.remove(val)
    if len(root) == 0:
        ssl = True
    else:
        value = root[0][1].text
        if value == "true":
            ssl = True
        elif value == "false":
            ssl = False
        else:
            ssl = True
    return ssl


def clusterName(
    version,
    cloudera_manager_host_ip,
    cloudera_manager_port,
    cloudera_manager_username,
    cloudera_manager_password,
):
    """Get Cluster Name from User.

    Args:
        version (int): Cloudera distributed Hadoop version
        cloudera_manager_host_ip (str): Cloudera Manager Host IP.
        cloudera_manager_port (str): Cloudera Manager Port Number.
        cloudera_manager_username (str): Cloudera Manager Username.
        cloudera_manager_password (str): Cloudera Manager Password.

    Returns:
        cluster_name (str): Cluster name present in cloudera manager.

    """

    initial_run = None
    if version == 7:
        initial_run = requests.get(
            "http://{}:{}/api/v41/clusters".format(
                cloudera_manager_host_ip, cloudera_manager_port
            ),
            auth=HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password),
        )
    elif version == 6:
        initial_run = requests.get(
            "http://{}:{}/api/v19/clusters".format(
                cloudera_manager_host_ip, cloudera_manager_port
            ),
            auth=HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password),
        )
    elif version == 5:
        initial_run = requests.get(
            "http://{}:{}/api/v19/clusters".format(
                cloudera_manager_host_ip, cloudera_manager_port
            ),
            auth=HTTPBasicAuth(cloudera_manager_username, cloudera_manager_password),
        )
    cluster = initial_run.json()
    cluster_items = cluster["items"]
    index_count = 0
    cluster_dt = pd.DataFrame()
    input_index = 1
    for name in cluster_items:
        cluster_temp = pd.DataFrame(
            {"Index": input_index, "Name": name["name"]}, index=[index_count]
        )
        cluster_dt = cluster_dt.append(cluster_temp)
        input_index = input_index + 1
    print("Select cluster name from list below : ")
    for ind in cluster_dt.index:
        print(cluster_dt["Index"][ind], ".", cluster_dt["Name"][ind])
    var = int(input("Enter serial number for selected cluster name : "))
    name_list = cluster_dt["Index"].tolist()
    cluster_name = None
    if var in name_list:
        cluster_name = cluster_dt[cluster_dt["Index"] == var].Name.iloc[0]
        print("This cluster is selected : ", cluster_name)
    else:
        print("Wrong Input! Try Again")
        print("Select cluster name from list below : ")
        for ind in cluster_dt.index:
            print(cluster_dt["Index"][ind], ".", cluster_dt["Name"][ind])
        var = int(input("Enter serial number for selected cluster name : "))
        name_list = cluster_dt["Index"].tolist()
        cluster_name = None
        if var in name_list:
            cluster_name = cluster_dt[cluster_dt["Index"] == var].Name.iloc[0]
            print("This cluster is selected : ", cluster_name)
        else:
            print("Wrong Input! Try Again")
            exit()
    return cluster_name



def BrokerListInput():
    broker_list = []
    t = input("Do you want to enter Kafka credentials? [y/n] ")
    if t in ["y", "Y"]:
        broker_list = []
        n = int(input("Enter number of brokers: "))
        for i in range(0, n):
            broker = {"host":"", "port":"", "log_dir":""}
            broker["host"] = input("Enter the hostname or IP of broker {}: ".format(i))
            t1 = input("Is your broker hosted on {} have port number 9092? [y/n] ".format(broker["host"]))
            if t1 in ["n", "N"]:
                broker["port"] = input("Enter the port of broker hosted on {}: ".format(broker["host"]))
            elif t1 in ["y", "Y"]:
                broker["port"] = "9092"
            else:
                print("Wrong Input! Try Again")
                t2 = input("Is your broker hosted on {} have port number 9092? [y/n] ".format(broker["host"]))
                if t2 in ["n", "N"]:
                    broker["port"] = input("Enter the port of broker hosted on {}: ".format(broker["host"]))
                elif t2 in ["y", "Y"]:
                    broker["port"] = "9092"
                else:
                    print("Wrong Input!")
                    exit()
            t1 = input("Is your broker hosted on {} have log directory path /var/local/kafka/data/? [y/n] ".format(broker["host"]))
            if t1 in ["n", "N"]:
                broker["log_dir"] = input("Enter the log directory path of broker hosted on {}: ".format(broker["host"]))
            elif t1 in ["y", "Y"]:
                broker["log_dir"] = "/var/local/kafka/data/"
            else:
                print("Wrong Input! Try Again")
                t2 = input("Is your broker hosted on {} have log directory path /var/local/kafka/data/? [y/n] ".format(broker["host"]))
                if t2 in ["n", "N"]:
                    broker["log_dir"] = input("Enter the log directory path of broker hosted on {}: ".format(broker["host"]))
                elif t2 in ["y", "Y"]:
                    broker["log_dir"] = "/var/local/kafka/data/"
                else:
                    print("Wrong Input!")
                    exit()
            broker_list.append(broker)
    elif t in ["n", "N"]:
        broker_list = []
    else:
        if t in ["y", "Y"]:
            broker_list = []
            n = int(input("Enter number of brokers: "))
            for i in range(0, n):
                broker = {"host":"", "port":"", "log_dir":""}
                broker["host"] = input("Enter the hostname or IP of broker {}: ".format(i))
                t1 = input("Is your broker hosted on {} have port number 9092? [y/n] ".format(broker["host"]))
                if t1 in ["n", "N"]:
                    broker["port"] = input("Enter the port of broker hosted on {}: ".format(broker["host"]))
                elif t1 in ["y", "Y"]:
                    broker["port"] = "9092"
                else:
                    print("Wrong Input! Try Again")
                    t2 = input("Is your broker hosted on {} have port number 9092? [y/n] ".format(broker["host"]))
                    if t2 in ["n", "N"]:
                        broker["port"] = input("Enter the port of broker hosted on {}: ".format(broker["host"]))
                    elif t2 in ["y", "Y"]:
                        broker["port"] = "9092"
                    else:
                        print("Wrong Input!")
                        exit()
                t1 = input("Is your broker hosted on {} have log directory path /var/local/kafka/data/? [y/n] ".format(broker["host"]))
                if t1 in ["n", "N"]:
                    broker["log_dir"] = input("Enter the log directory path of broker hosted on {}: ".format(broker["host"]))
                elif t1 in ["y", "Y"]:
                    broker["log_dir"] = "/var/local/kafka/data/"
                else:
                    print("Wrong Input! Try Again")
                    t2 = input("Is your broker hosted on {} have log directory path /var/local/kafka/data/? [y/n] ".format(broker["host"]))
                    if t2 in ["n", "N"]:
                        broker["log_dir"] = input("Enter the log directory path of broker hosted on {}: ".format(broker["host"]))
                    elif t2 in ["y", "Y"]:
                        broker["log_dir"] = "/var/local/kafka/data/"
                    else:
                        print("Wrong Input!")
                        exit()
                broker_list.append(broker)
        elif t in ["n", "N"]:
            broker_list = []
        else:
            print("Wrong Input!")
            exit()
    return broker_list

def getInput(version):
    """Get input from user related to cloudera manager like Host Ip, Username, 
    Password and Cluster Name.

    Args:
        version (int): Cloudera distributed Hadoop version
    Returns:
        inputs (dict): Contains user input attributes

    """

    inputs = {}
    inputs["version"] = version
    inputs["ssl"] = checkSSL()

    if inputs["ssl"]:
        print("Enter details accordingly as SSL is enabled.")
    else:
        print("Enter details accordingly as SSL is disabled.")
    t = input("Do you want to enter clouder manager credentials? [y/n] ")
    if t in ["y", "Y"]:
        inputs["cloudera_manager_host_ip"] = input("Enter Cloudera Manager Host IP: ")
        t1 = input("Is your Cloudera Manager Port number 7180? [y/n] ")
        if t1 in ["y", "Y"]:
            inputs["cloudera_manager_port"] = "7180"
        elif t1 in ["n", "N"]:
            inputs["cloudera_manager_port"] = input("Enter Cloudera Manager Port : ")
        else:
            print("Wrong Input! Try Again")
            t1 = input("Is your Cloudera Manager Port number 7180? [y/n] ")
            if t1 in ["y", "Y"]:
                inputs["cloudera_manager_port"] = "7180"
            elif t1 in ["n", "N"]:
                inputs["cloudera_manager_port"] = input(
                    "Enter Cloudera Manager Port : "
                )
            else:
                print("Wrong Input! Try Again")
                exit()
        inputs["cloudera_manager_username"] = input("Enter Cloudera Manager Username: ")
        inputs["cloudera_manager_password"] = getpass(
            prompt="Enter Cloudera Manager Password: "
        )
        inputs["cluster_name"] = clusterName(
            inputs["version"],
            inputs["cloudera_manager_host_ip"],
            inputs["cloudera_manager_port"],
            inputs["cloudera_manager_username"],
            inputs["cloudera_manager_password"],
        )
    elif t in ["n", "N"]:
        inputs["cloudera_manager_host_ip"] = None
        inputs["cloudera_manager_port"] = None
        inputs["cloudera_manager_username"] = None
        inputs["cloudera_manager_password"] = None
        inputs["cluster_name"] = None
    else:
        print("Wrong Input! Try Again")
        t = input("Do you want to enter clouder manager credentials? [y/n] ")
        if t in ["y", "Y"]:
            inputs["cloudera_manager_host_ip"] = input(
                "Enter Cloudera Manager Host IP: "
            )
            t1 = input("Is your Cloudera Manager Port number 7180? [y/n] ")
            if t1 in ["y", "Y"]:
                inputs["cloudera_manager_port"] = "7180"
            elif t1 in ["n", "N"]:
                inputs["cloudera_manager_port"] = input(
                    "Enter Cloudera Manager Port : "
                )
            else:
                print("Wrong Input! Try Again")
                t1 = input("Is your Cloudera Manager Port number 7180? [y/n] ")
                if t1 in ["y", "Y"]:
                    inputs["cloudera_manager_port"] = "7180"
                elif t1 in ["n", "N"]:
                    inputs["cloudera_manager_port"] = input(
                        "Enter Cloudera Manager Port : "
                    )
                else:
                    print("Wrong Input! Try Again")
                    exit()
            inputs["cloudera_manager_username"] = input(
                "Enter Cloudera Manager Username: "
            )
            inputs["cloudera_manager_password"] = getpass(
                prompt="Enter Cloudera Manager Password: "
            )
            inputs["cluster_name"] = clusterName(
                inputs["version"],
                inputs["cloudera_manager_host_ip"],
                inputs["cloudera_manager_port"],
                inputs["cloudera_manager_username"],
                inputs["cloudera_manager_password"],
            )
        elif t in ["n", "N"]:
            inputs["cloudera_manager_host_ip"] = None
            inputs["cloudera_manager_port"] = None
            inputs["cloudera_manager_username"] = None
            inputs["cloudera_manager_password"] = None
            inputs["cluster_name"] = None
        else:
            print("Wrong Input! Try Again")
            exit()
    t = input("Do you want to enter Hive credentials? [y/n] ")
    if t in ["y", "Y"]:
        inputs["hive_username"] = input("Enter Hive Metastore Username: ")
        inputs["hive_password"] = getpass(prompt="Enter Hive Metastore Password: ")
    elif t in ["n", "N"]:
        inputs["hive_username"] = None
        inputs["hive_password"] = None
    else:
        print("Wrong Input! Try Again")
        t = input("Do you want to enter Hive credentials? [y/n] ")
        if t in ["y", "Y"]:
            inputs["hive_username"] = input("Enter Hive Metastore Username: ")
            inputs["hive_password"] = getpass(prompt="Enter Hive Metastore Password: ")
        elif t in ["n", "N"]:
            inputs["hive_username"] = None
            inputs["hive_password"] = None
        else:
            print("Wrong Input! Try Again")
            exit()
    inputs["broker_list"] = BrokerListInput()
    print("Select date range from list below : ")
    print("1. Week\n2. Month\n3. Custom")
    t = int(input("Enter serial number for selected date range: "))
    if t == 1:
        inputs["start_date"] = (datetime.now() - timedelta(days=7)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        inputs["end_date"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    elif t == 2:
        inputs["start_date"] = (datetime.now() - timedelta(days=30)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        inputs["end_date"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    elif t == 3:
        inputs["start_date"] = datetime.strptime(
            input("Enter Start Date (YYYY-MM-DD HH:MM) : "), "%Y-%m-%d %H:%M"
        ).strftime("%Y-%m-%dT%H:%M:%S")
        inputs["end_date"] = datetime.strptime(
            input("Enter End Date (YYYY-MM-DD HH:MM) : "), "%Y-%m-%d %H:%M"
        ).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        print("Wrong Input! Try Again")
        print("Select date range from list below : ")
        print("1. Week\n2. Month\n3. Custom")
        t = int(input("Enter serial number for selected date range: "))
        if t == 1:
            inputs["start_date"] = (datetime.now() - timedelta(days=7)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            inputs["end_date"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        elif t == 2:
            inputs["start_date"] = (datetime.now() - timedelta(days=30)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            inputs["end_date"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        elif t == 3:
            inputs["start_date"] = datetime.strptime(
                input("Enter Start Date (YYYY-MM-DD HH:MM) : "), "%Y-%m-%d %H:%M"
            ).strftime("%Y-%m-%dT%H:%M:%S")
            inputs["end_date"] = datetime.strptime(
                input("Enter End Date (YYYY-MM-DD HH:MM) : "), "%Y-%m-%d %H:%M"
            ).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            print("Wrong Input! Try Again")
            exit()
    # if inputs["ssl"]:
    #     inputs["cloudera_manager_port"] = 7183
    # else:
    #     inputs["cloudera_manager_port"] = 7180
    # inputs["hive_username"] = "hive"
    # if version == 7:
    #     inputs["cloudera_manager_host_ip"] = "10.0.0.239"
    #     inputs["cluster_name"] = "MSTECH"
    #     inputs["cloudera_manager_username"] = "admin"
    #     inputs["cloudera_manager_password"] = "admin"
    #     inputs["hive_password"] = "MsSGI2lC9l"
    # elif version == 6:
    #     inputs["cloudera_manager_host_ip"] = "10.0.0.19"
    #     inputs["cluster_name"] = "QPTECH"
    #     inputs["cloudera_manager_username"] = "admin"
    #     inputs["cloudera_manager_password"] = "admin"
    #     inputs["hive_password"] = "pDkiWaKQB6"
    # elif version == 5:
    #     inputs["cloudera_manager_host_ip"] = "10.0.0.23"
    #     inputs["cluster_name"] = "cluster"
    #     inputs["cloudera_manager_username"] = "admin"
    #     inputs["cloudera_manager_password"] = "admin"
    #     inputs["hive_password"] = "4AH9FK2zk8"
    return inputs


def getLogger():
    """Defining custom logger object with custom formatter and file handler.

    Returns:
        logger (obj): Custom logger object
    """

    logger = logging.getLogger("hadoop_assessment_tool")
    handler = logging.FileHandler(
        "hadoop_assessment_tool_{}.log".format(datetime.now())
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger
