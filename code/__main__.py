import time

p_start = time.time()
from imports import *
from PdfGenerator import *

global logger
logger = getLogger()
hversion = os.popen("hadoop version").read()
if "CDH-7" in hversion:
    inputs = getInput(7)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run_7()
elif "cdh6" in hversion:
    inputs = getInput(6)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run_6()
elif "cdh5" in hversion:
    inputs = getInput(5)
    inputs["logger"] = logger
    obj = PdfGenerator(inputs)
    obj.run_5()
p_end = time.time()
print(p_end - p_start)
