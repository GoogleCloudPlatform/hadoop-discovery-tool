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
from pyhive import hive
from pprint import pprint
from fpdf import FPDF
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
sns.set(rc={'figure.figsize':(15, 5)})
pd.set_option('display.max_colwidth', 0)
pd.options.display.float_format = "{:,.2f}".format
warnings.filterwarnings("ignore")
sns.set(rc={'figure.figsize':(15, 5)})
date_range_start = datetime(2021, 1, 15, 0, 0, 0, 0)
date_range_end = datetime(2021, 2, 8, 18, 0, 0, 0)
start_date = date_range_start.strftime('%Y-%m-%dT%H:%M:%S')
end_date = date_range_end.strftime('%Y-%m-%dT%H:%M:%S')
# version = 7
# cloudera_manager_username = 'admin'
# cloudera_manager_password = 'admin'
# cloudera_manager_host_ip = '10.0.0.16'
# version = 6
# cloudera_manager_username = 'admin'
# cloudera_manager_password = 'admin'
# cloudera_manager_host_ip = '10.0.0.19'
version = 5
cloudera_manager_username = 'admin'
cloudera_manager_password = 'admin'
cloudera_manager_host_ip = '10.0.0.23'
