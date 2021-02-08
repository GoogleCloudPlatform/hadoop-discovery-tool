import time
p_start = time.time()
from imports import *
from PdfGenerator import *
hversion=os.popen('hadoop version').read()
if('CDH-7' in hversion):
    obj = PdfGenerator
    obj.run_7()
elif('cdh6' in hversion):
    obj = PdfGenerator
    obj.run_6()
elif('cdh5' in hversion):
    obj = PdfGenerator
    obj.run_5()
p_end = time.time()
print(p_end - p_start)