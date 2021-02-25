# ------------------------------------------------------------------------------
# Main module will decide the flow of the based on the cloudera manager
# installed on the cluster and it will check forthe version installed
# accordingly the code will generate the custom report.
# ------------------------------------------------------------------------------

# Importing required libraries
import time

p_start = time.time()
from imports import *
from PdfGenerator import *

# Creating logger object
logger = getLogger()

# Get Cloudera Distributed Hadoop Version
hversion = os.popen("hadoop version").read()

# Direct code to respective function based on cloudera version
# Get User Input
if "CDH-7" in hversion:
    inputs = getInput(7)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run()
elif "cdh6" in hversion:
    inputs = getInput(6)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run()
elif "cdh5" in hversion:
    inputs = getInput(5)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run()
p_end = time.time()
print(p_end - p_start)
