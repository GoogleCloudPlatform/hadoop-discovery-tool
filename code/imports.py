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


def check_ssl():
    """Check whether SSL is enabled or not

    Returns:
        ssl (bool): SSL flag
    """

    ssl = None
    if path.exists("/etc/hadoop/conf/core-site.xml"):
        hadoop_path = "/etc/hadoop/conf/core-site.xml"
    elif path.exists("/etc/hive/conf/core-site.xml"):
        hadoop_path = "/etc/hive/conf/core-site.xml"
    else:
        return ssl
    xml_data = os.popen("cat {}".format(hadoop_path)).read()
    root = ET.fromstring(xml_data)
    for val in root.findall("property"):
        name = val.find("name").text
        if "hadoop.ssl.enabled" not in name:
            root.remove(val)
    if len(root) == 0:
        ssl = None
    else:
        value = root[0][1].text
        if value == "true":
            ssl = True
        elif value == "false":
            ssl = False
        else:
            ssl = True
    return ssl


def check_config_path():
    """Check whether configuration files exists or not

    Returns:
        config_path (dict): config paths
    """

    config_path = {}
    if path.exists("/etc/hadoop/conf/core-site.xml"):
        config_path["core"] = "/etc/hadoop/conf/core-site.xml"
    else:
        if path.exists("/etc/hive/conf/core-site.xml"):
            config_path["core"] = "/etc/hive/conf/core-site.xml"
        else:
            config_path["core"] = None
    if path.exists("/etc/hadoop/conf/yarn-site.xml"):
        config_path["yarn"] = "/etc/hadoop/conf/yarn-site.xml"
    else:
        if path.exists("/etc/hive/conf/yarn-site.xml"):
            config_path["yarn"] = "/etc/hive/conf/yarn-site.xml"
        else:
            config_path["yarn"] = None
    if path.exists("/etc/hadoop/conf/mapred-site.xml"):
        config_path["mapred"] = "/etc/hadoop/conf/mapred-site.xml"
    else:
        if path.exists("/etc/hive/conf/mapred-site.xml"):
            config_path["mapred"] = "/etc/hive/conf/mapred-site.xml"
        else:
            config_path["mapred"] = None
    if path.exists("/etc/hadoop/conf/hdfs-site.xml"):
        config_path["hdfs"] = "/etc/hadoop/conf/hdfs-site.xml"
    else:
        if path.exists("/etc/hive/conf/hdfs-site.xml"):
            config_path["hdfs"] = "/etc/hive/conf/hdfs-site.xml"
        else:
            config_path["hdfs"] = None
    if path.exists("/etc/hive/conf/hive-site.xml"):
        config_path["hive"] = "/etc/hive/conf/hive-site.xml"
    else:
        config_path["hive"] = None
    if path.exists("/etc/spark/conf/spark-defaults.conf"):
        config_path["spark"] = "/etc/spark/conf/spark-defaults.conf"
    else:
        config_path["spark"] = None
    if path.exists("/etc/kafka/conf/kafka-client.conf"):
        config_path["kafka"] = "/etc/kafka/conf/kafka-client.conf"
    else:
        config_path["kafka"] = None
    return config_path


def get_cloudera_creds(version, ssl):
    """Get input from user related to cloudera manager.

    Returns:
        inputs (dict): Contains user input attributes

    """

    try:
        c = 3
        while c > 0:
            print(
                "A major number of metrics generation would require Cloudera manager credentials"
            )
            print(
                "Therefore, would you be able to provide your Cloudera Manager credentials? [y/n]:"
            )
            t = input()
            if t in ["y", "Y"]:
                print("Enter Cloudera Manager Host IP: ")
                host = input()
                c1 = 3
                while c1 > 0:
                    print("Is your Cloudera Manager Port number 7180? [y/n] ")
                    t1 = input()
                    if t1 in ["y", "Y"]:
                        port = "7180"
                    elif t1 in ["n", "N"]:
                        print("Enter Cloudera Manager Port: ")
                        port = input()
                    c1 = c1 - 1
                    if c1 == 0:
                        print("Incorrect Input!")
                        exit()
                    else:
                        print("Incorrect Input! Try Again")
                print("Enter Cloudera Manager Username: ")
                uname = input()
                password = getpass(prompt="Enter Cloudera Manager Password: ")
                return host, port, uname, password
            elif t in ["n", "N"]:
                return None, None, None, None
            c = c - 1
            if c == 0:
                print("Incorrect Input!")
                exit()
            else:
                print("Incorrect Input! Try Again")
    except Exception as e:
        return None, None, None, None


def cloudera_cluster_name(
    version,
    ssl,
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

    try:
        initial_run = None
        http = None
        if ssl:
            http = "https"
        else:
            http = "http"
        if version == 7:
            initial_run = requests.get(
                "{}://{}:{}/api/v41/clusters".format(
                    http, cloudera_manager_host_ip, cloudera_manager_port
                ),
                auth=HTTPBasicAuth(
                    cloudera_manager_username, cloudera_manager_password
                ),
            )
        elif version == 6:
            initial_run = requests.get(
                "{}://{}:{}/api/v19/clusters".format(
                    http, cloudera_manager_host_ip, cloudera_manager_port
                ),
                auth=HTTPBasicAuth(
                    cloudera_manager_username, cloudera_manager_password
                ),
            )
        elif version == 5:
            initial_run = requests.get(
                "{}://{}:{}/api/v19/clusters".format(
                    http, cloudera_manager_host_ip, cloudera_manager_port
                ),
                auth=HTTPBasicAuth(
                    cloudera_manager_username, cloudera_manager_password
                ),
            )
        else:
            print("Unable to fetch cloudera clusters as cloudera does not exist")
            return None
        if initial_run.status_code == 200:
            cluster = initial_run.json()
            cluster_list = []
            for i in cluster["items"]:
                cluster_list.append(i["name"])
            c = 3
            while c > 0:
                print("Select cluster name from list below: ")
                for i in range(len(cluster_list)):
                    print(str(i + 1) + ". " + cluster_list[i])
                print(
                    "Enter the serial number(1/2/../n) for the selected cluster name: "
                )
                var = input()
                if var.numeric():
                    var = int(var)
                    if (var > 0) and (var <= len(cluster_list)):
                        break
                c = c - 1
                if c == 0:
                    print("Incorrect Input!")
                    exit()
                else:
                    print("Incorrect Input! Try Again")
            cluster_name = cluster_list[var - 1]
            print("This cluster is selected: " + cluster_name)
            return cluster_name
        else:
            print(
                "Cloudera credentials are incorrect or unable to connect to Cloudera Manager!"
            )
            return None
    except Exception as e:
        print(
            "Cloudera credentials are incorrect or unable to connect to Cloudera Manager!"
        )
        return None


def get_hive_creds(inputs):
    """Get input from user related to Hive.

    Returns:
        inputs (dict): Contains user input attributes

    """

    try:
        c = 3
        while c > 0:
            print("Do you want to enter Hive credentials? [y/n] ")
            t = input()
            if t in ["y", "Y"]:
                print("Enter Hive Metastore Username: ")
                hive_username = input()
                hive_password = getpass(prompt="Enter Hive Metastore Password: ")
                r = None
                http = None
                if inputs["ssl"]:
                    http = "https"
                else:
                    http = "http"
                if inputs["version"] == 7:
                    r = requests.get(
                        "{}://{}:{}/api/v41/clusters/{}/services/hive/config".format(
                            http,
                            inputs["cloudera_manager_host_ip"],
                            inputs["cloudera_manager_port"],
                            inputs["cluster_name"],
                        ),
                        auth=HTTPBasicAuth(
                            inputs["cloudera_manager_username"],
                            inputs["cloudera_manager_password"],
                        ),
                        verify=False,
                    )
                elif inputs["version"] == 6:
                    r = requests.get(
                        "{}://{}:{}/api/v19/clusters/{}/services/hive/config".format(
                            http,
                            inputs["cloudera_manager_host_ip"],
                            inputs["cloudera_manager_port"],
                            inputs["cluster_name"],
                        ),
                        auth=HTTPBasicAuth(
                            inputs["cloudera_manager_username"],
                            inputs["cloudera_manager_password"],
                        ),
                        verify=False,
                    )
                elif inputs["version"] == 5:
                    r = requests.get(
                        "{}://{}:{}/api/v19/clusters/{}/services/hive/config".format(
                            http,
                            inputs["cloudera_manager_host_ip"],
                            inputs["cloudera_manager_port"],
                            inputs["cluster_name"],
                        ),
                        auth=HTTPBasicAuth(
                            inputs["cloudera_manager_username"],
                            inputs["cloudera_manager_password"],
                        ),
                        verify=False,
                    )
                else:
                    return None, None
                if r.status_code == 200:
                    hive_config = r.json()
                    hive_config_items = hive_config["items"]
                    mt_db_host = ""
                    mt_db_name = ""
                    mt_db_type = ""
                    mt_db_port = ""
                    for i in hive_config_items:
                        if i["name"] == "hive_metastore_database_host":
                            mt_db_host = i["value"]
                        elif i["name"] == "hive_metastore_database_name":
                            mt_db_name = i["value"]
                        elif i["name"] == "hive_metastore_database_port":
                            mt_db_port = i["value"]
                        elif i["name"] == "hive_metastore_database_type":
                            mt_db_type = i["value"]
                    if mt_db_type == "postgresql":
                        database_uri = "postgres+psycopg2://{}:{}@{}:{}/{}".format(
                            hive_username,
                            hive_password,
                            mt_db_host,
                            mt_db_port,
                            mt_db_name,
                        )
                        engine = create_engine(database_uri)
                        result = engine.execute("""select * from "DBS";""")
                    if mt_db_type == "mysql":
                        database_uri = "mysql+pymysql://{}:{}@{}:{}/{}".format(
                            hive_username,
                            hive_password,
                            mt_db_host,
                            mt_db_port,
                            mt_db_name,
                        )
                        engine = create_engine(database_uri)
                        result = engine.execute("""select * from DBS;""")
                    f = False
                    for row in result:
                        f = True
                    if f:
                        return hive_username, hive_password
                    else:
                        print("Unable to connect to Hive!")
                else:
                    print("Unable to connect to Cloudera Manager!")
            elif t in ["n", "N"]:
                return None, None
            c = c - 1
            if c == 0:
                print("Incorrect Input!")
                return None, None
            else:
                print("Incorrect Input! Try Again")
    except Exception as e:
        return None, None


def broker_list_input():
    try:
        broker_list = []
        print("Do you want to enter Kafka credentials? [y/n] ")
        t = input()
        if t in ["y", "Y"]:
            broker_list = []
            print("Enter number of brokers: ")
            n = input()
            if not n.isnumeric():
                print(
                    "Wrong Input! Number of brokers should be positive integer! Try Again"
                )
                print("Enter number of brokers: ")
                n = input()
                if not n.isnumeric():
                    print("Wrong Input!")
                    exit()
            n = int(n)
            for i in range(0, n):
                broker = {"host": "", "port": "", "log_dir": ""}
                print("Enter the hostname or IP of broker {}: ".format(i + 1))
                broker["host"] = input()
                print(
                    "Is your broker hosted on {} have port number 9092? [y/n] ".format(
                        broker["host"]
                    )
                )
                t1 = input()
                if t1 in ["n", "N"]:
                    print(
                        "Enter the port of broker hosted on {}: ".format(broker["host"])
                    )
                    broker["port"] = input()
                elif t1 in ["y", "Y"]:
                    broker["port"] = "9092"
                else:
                    print("Wrong Input! Try Again")
                    print(
                        "Is your broker hosted on {} have port number 9092? [y/n] ".format(
                            broker["host"]
                        )
                    )
                    t2 = input()
                    if t2 in ["n", "N"]:
                        print(
                            "Enter the port of broker hosted on {}: ".format(
                                broker["host"]
                            )
                        )
                        broker["port"] = input()
                    elif t2 in ["y", "Y"]:
                        broker["port"] = "9092"
                    else:
                        print("Wrong Input!")
                        exit()
                print(
                    "Is your broker hosted on {} have log directory path /var/local/kafka/data/? [y/n] ".format(
                        broker["host"]
                    )
                )
                t1 = input()
                if t1 in ["n", "N"]:
                    print(
                        "Enter the log directory path of broker hosted on {}: ".format(
                            broker["host"]
                        )
                    )
                    broker["log_dir"] = input()
                elif t1 in ["y", "Y"]:
                    broker["log_dir"] = "/var/local/kafka/data/"
                else:
                    print("Wrong Input! Try Again")
                    print(
                        "Is your broker hosted on {} have log directory path /var/local/kafka/data/? [y/n] ".format(
                            broker["host"]
                        )
                    )
                    t2 = input()
                    if t2 in ["n", "N"]:
                        print(
                            "Enter the log directory path of broker hosted on {}: ".format(
                                broker["host"]
                            )
                        )
                        broker["log_dir"] = input()
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
                print("Enter number of brokers: ")
                n = input()
                if not n.isnumeric():
                    print(
                        "Wrong Input! Number of brokers should be positive integer! Try Again"
                    )
                    print("Enter number of brokers: ")
                    n = input()
                    if not n.isnumeric():
                        print("Wrong Input!")
                        exit()
                n = int(n)
                for i in range(0, n):
                    broker = {"host": "", "port": "", "log_dir": ""}
                    print("Enter the hostname or IP of broker {}: ".format(i + 1))
                    broker["host"] = input()
                    print(
                        "Is your broker hosted on {} have port number 9092? [y/n] ".format(
                            broker["host"]
                        )
                    )
                    t1 = input()
                    if t1 in ["n", "N"]:
                        print(
                            "Enter the port of broker hosted on {}: ".format(
                                broker["host"]
                            )
                        )
                        broker["port"] = input()
                    elif t1 in ["y", "Y"]:
                        broker["port"] = "9092"
                    else:
                        print("Wrong Input! Try Again")
                        print(
                            "Is your broker hosted on {} have port number 9092? [y/n] ".format(
                                broker["host"]
                            )
                        )
                        t2 = input()
                        if t2 in ["n", "N"]:
                            print(
                                "Enter the port of broker hosted on {}: ".format(
                                    broker["host"]
                                )
                            )
                            broker["port"] = input()
                        elif t2 in ["y", "Y"]:
                            broker["port"] = "9092"
                        else:
                            print("Wrong Input!")
                            exit()
                    print(
                        "Is your broker hosted on {} have log directory path /var/local/kafka/data/? [y/n] ".format(
                            broker["host"]
                        )
                    )
                    t1 = input()
                    if t1 in ["n", "N"]:
                        print(
                            "Enter the log directory path of broker hosted on {}: ".format(
                                broker["host"]
                            )
                        )
                        broker["log_dir"] = input()
                    elif t1 in ["y", "Y"]:
                        broker["log_dir"] = "/var/local/kafka/data/"
                    else:
                        print("Wrong Input! Try Again")
                        print(
                            "Is your broker hosted on {} have log directory path /var/local/kafka/data/? [y/n] ".format(
                                broker["host"]
                            )
                        )
                        t2 = input()
                        if t2 in ["n", "N"]:
                            print(
                                "Enter the log directory path of broker hosted on {}: ".format(
                                    broker["host"]
                                )
                            )
                            broker["log_dir"] = input()
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
    except Exception as e:
        return []


def get_input(version):
    """Get input from user related to cloudera manager like Host Ip, Username, 
    Password and Cluster Name.

    Args:
        version (int): Cloudera distributed Hadoop version
    Returns:
        inputs (dict): Contains user input attributes

    """

    try:
        inputs = {}
        inputs["version"] = version
        inputs["ssl"] = check_ssl()
        if inputs["ssl"] == None:
            c = 3
            while c > 0:
                print("Do you have SSL enabled for your cluster? [y/n] ")
                t = input()
                if t in ["y", "Y"]:
                    inputs["ssl"] = True
                    break
                elif t in ["n", "N"]:
                    inputs["ssl"] = False
                    break
                c = c - 1
                if c == 0:
                    print("Incorrect Input!")
                    exit()
                else:
                    print("Incorrect Input! Try Again")
        else:
            if inputs["ssl"]:
                print("Enter details accordingly as SSL is enabled.")
            else:
                print("Enter details accordingly as SSL is disabled.")
        inputs["config_path"] = check_config_path()
        if inputs["version"] != 0:
            c = 3
            while c > 0:
                (
                    inputs["cloudera_manager_host_ip"],
                    inputs["cloudera_manager_port"],
                    inputs["cloudera_manager_username"],
                    inputs["cloudera_manager_password"],
                ) = get_cloudera_creds()
                if inputs["cloudera_manager_host_ip"] == None:
                    inputs["cluster_name"] = None
                    break
                inputs["cluster_name"] = cloudera_cluster_name(
                    inputs["version"],
                    inputs["ssl"],
                    inputs["cloudera_manager_host_ip"],
                    inputs["cloudera_manager_port"],
                    inputs["cloudera_manager_username"],
                    inputs["cloudera_manager_password"],
                )
                if inputs["cluster_name"] == None:
                    c = c - 1
                    if c == 0:
                        print("Incorrect Input!")
                        exit()
                    else:
                        print("Incorrect Input! Try Again")
                else:
                    break
        if inputs["cloudera_manager_host_ip"] == None:
            inputs["hive_username"], inputs["hive_password"] = None, None
        else:
            inputs["hive_username"], inputs["hive_password"] = get_hive_creds(inputs)
        inputs["broker_list"] = broker_list_input()
        print("Select date range from list below : ")
        print("1. Week\n2. Month\n3. Custom")
        print("Enter serial number for selected date range: ")
        t = int(input())
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
            print("Enter Start Date (YYYY-MM-DD HH:MM): ")
            inputs["start_date"] = datetime.strptime(
                input(), "%Y-%m-%d %H:%M"
            ).strftime("%Y-%m-%dT%H:%M:%S")
            print("Enter End Date (YYYY-MM-DD HH:MM): ")
            inputs["end_date"] = datetime.strptime(input(), "%Y-%m-%d %H:%M").strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        else:
            print("Wrong Input! Try Again")
            print("Select date range from list below : ")
            print("1. Week\n2. Month\n3. Custom")
            print("Enter serial number for selected date range: ")
            t = int(input())
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
                print("Enter Start Date (YYYY-MM-DD HH:MM): ")
                inputs["start_date"] = datetime.strptime(
                    input(), "%Y-%m-%d %H:%M"
                ).strftime("%Y-%m-%dT%H:%M:%S")
                print("Enter End Date (YYYY-MM-DD HH:MM): ")
                inputs["end_date"] = datetime.strptime(
                    input(), "%Y-%m-%d %H:%M"
                ).strftime("%Y-%m-%dT%H:%M:%S")
            else:
                print("Wrong Input!")
                exit()
        return inputs
    except Exception as e:
        return {}


def get_logger():
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
