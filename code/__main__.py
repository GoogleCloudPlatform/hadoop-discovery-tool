# ------------------------------------------------------------------------------
# Main module will decide the program flow. It will connect with the
# cloudera manager and it checks for the installed cloudera version.
# According to the version, program flow will proceed and generate the PDF
# report.
# ------------------------------------------------------------------------------

# Importing required libraries
from imports import *
from PdfGenerator import *

# Creating logger object
logger = get_logger()

# Get Cloudera Distribution and Hadoop Version
hversion = os.popen("hadoop version").read()

# Direct code to respective function based on cloudera version
# Get User Input
if "CDH-7" in hversion:
    inputs = get_input(7)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run()
elif "cdh6" in hversion:
    inputs = get_input(6)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run()
elif "cdh5" in hversion:
    inputs = get_input(5)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run()
else:
    inputs = get_input(0)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run()

if os.path.exists("../../hadoop_assessment_report.pdf"):
    response = "Hadoop Assessment tool has run successfully, PDF report is generated\nFollowing is the PDF path: {}".format(
        os.path.abspath("../../hadoop_assessment_report.pdf")
    )
else:
    response = "Unable to generate PDF report, check logs for more details"
print(response)
