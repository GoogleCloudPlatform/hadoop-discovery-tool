# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import sys

# initialise flags to identify packages
nload_dt, vnstat_dt, gcc_dt, odbc_dt, sasl_dt, pydevel_dt, iostat_dt = (
    "",
    "",
    "",
    "",
    "",
    "",
    "",
)
# list to hold installed and non installed data
installed, not_installed = [], []
# This command will fetch os-name for ex. centos,debian,opensuse etc.
# os_name = os.popen("grep PRETTY_NAME /etc/os-release").read()
# os_name = os_name.lower()
final_version = None

vs, trash, name, dt, getpython, getpython1, getpython2 = "", "", "", "", "", "", ""

final_name = None
py_val = ""
flag = 0

py_val = os.popen(
    """python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))'"""
).read()
py_val = "".join([str(elem) for elem in py_val])
py_val = py_val.replace("\n", "")

try:
    vs = os.popen("lsb_release -r 2>/dev/null").read()
    trash, version = vs.split(":")
    final_version = version.replace('"', "")
    final_version = final_version.strip("\n")
    final_version = final_version.strip("\t")
    part_string = final_version.partition(".")
    final_version = float(part_string[0])
    os_id = os.popen("lsb_release -i 2>/dev/null").read()
    trash, os_identification = os_id.split(":")
    final_os_identification = os_identification.replace('"', "")
    final_os_identification = final_os_identification.strip("\n")
    final_name = final_os_identification.strip("\t")
    final_name = final_name.lower()
except Exception as e:
    pass

try:
    vs = os.popen("grep VERSION_ID /etc/os-release 2>/dev/null").read()
    trash, version = vs.split("=")
    final_version = version.replace('"', "")
    final_version = final_version.strip("\n")
    part_string = final_version.partition(".")
    final_version = float(part_string[0])
    dt = os.popen("grep ID= /etc/os-release 2>/dev/null").read()
    dt = dt.splitlines()
    for i in dt:
        if re.search(r"\b" "ID" r"\b", i):
            get_name = i

    trash, name = get_name.split("=")
    final_name = name.replace('"', "")
    final_name = final_name.strip("\n")
except Exception as e:
    pass


no_show = 0
"""
Here based on the os of the current system respective block will execute and will install packages
with their respective package managers.
"""
try:
    if final_name != type(None) and final_version != type(None):
        if "centos" in final_name and final_version >= 6:
            print("Checking OS Dependencies...")
            nload_dt = os.popen("rpm -qa 2>/dev/null | grep nload").read()
            vnstat_dt = os.popen("rpm -qa 2>/dev/null | grep vnstat").read()
            gcc_dt = os.popen("rpm -qa 2>/dev/null | grep gcc-c++").read()
            sasl_dt = os.popen("rpm -qa 2>/dev/null | grep cyrus-sasl-devel").read()
            odbc_dt = os.popen("rpm -qa 2>/dev/null | grep unixODBC-devel").read()
            pydevel_dt = os.popen(
                "rpm -qa 2>/dev/null | grep -e 'python\S*-dev'"
            ).read()
            iostat_dt = os.popen("rpm -qa 2>/dev/null | grep -e 'sysstat' ").read()
            venv_dt = "Venv"
        elif "ubuntu" in final_name and (final_version >= 16.0):
            print("Checking OS Dependencies...")
            if py_val == "3.8" and flag == 0:
                nload_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep nload"
                ).read()
                vnstat_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep vnstat"
                ).read()
                gcc_dt = os.popen("apt list --installed 2>/dev/null | grep g++*").read()
                sasl_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep sasl2-bin"
                ).read()
                odbc_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep unixodbc-dev"
                ).read()
                pydevel_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep -e 'python\S*-dev'"
                ).read()
                iostat_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep -e 'sysstat'"
                ).read()
                flag = 1
            if py_val == "3.7" and flag == 0:
                nload_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep nload"
                ).read()
                vnstat_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep vnstat"
                ).read()
                gcc_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep g++-*"
                ).read()
                sasl_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep sasl2-bin"
                ).read()
                odbc_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep unixodbc-dev"
                ).read()
                pydevel_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep -e 'python\S*-dev'"
                ).read()
                iostat_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep -e 'sysstat'"
                ).read()
                flag = 1
            if py_val == "3.6" and flag == 0:
                nload_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep nload"
                ).read()
                vnstat_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep vnstat"
                ).read()
                gcc_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep g++-*"
                ).read()
                sasl_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep sasl2-bin"
                ).read()
                odbc_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep unixodbc-dev"
                ).read()
                pydevel_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep -e 'python\S*-dev'"
                ).read()
                iostat_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep -e 'sysstat'"
                ).read()
                flag = 1
        elif "debian" in final_name and (final_version >= 8.9):
            print("Checking OS Dependencies...")
            if py_val >= "3.6" and flag == 0:
                nload_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep nload"
                ).read()
                vnstat_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep vnstat"
                ).read()
                gcc_dt = os.popen("apt list --installed 2>/dev/null | grep g++*").read()
                sasl_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep sasl2-bin"
                ).read()
                odbc_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep unixodbc-dev"
                ).read()
                pydevel_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep -e 'python\S*-dev'"
                ).read()
                iostat_dt = os.popen(
                    "apt list --installed 2>/dev/null | grep -e 'sysstat'"
                ).read()
                flag = 1
        elif "rhel" in final_name and final_version >= 7:
            print("Checking OS Dependencies...")
            nload_dt = os.popen("rpm -qa 2>/dev/null | grep nload").read()
            vnstat_dt = os.popen("rpm -qa 2>/dev/null| grep vnstat").read()
            gcc_dt = os.popen("rpm -qa 2>/dev/null | grep gcc-c++ ").read()
            sasl_dt = os.popen("rpm -qa 2>/dev/null | grep cyrus-sasl-devel ").read()
            odbc_dt = os.popen("rpm -qa 2>/dev/null | grep unixODBC-devel ").read()
            pydevel_dt = os.popen(
                "rpm -qa 2>/dev/null | grep -e 'python\S*-dev' "
            ).read()
            iostat_dt = os.popen("rpm -qa 2>/dev/null | grep -e 'sysstat' ").read()
        elif "sles" in final_name and final_version >= 12:
            print("Checking OS Dependencies...")
            nload_dt = os.popen("rpm -qa 2>/dev/null | grep nload ").read()
            vnstat_dt = os.popen("rpm -qa 2>/dev/null | grep vnstat ").read()
            gcc_dt = os.popen("rpm -qa 2>/dev/null | grep gcc-c++ ").read()
            sasl_dt = os.popen("rpm -qa 2>/dev/null | grep cyrus-sasl-devel ").read()
            odbc_dt = os.popen("rpm -qa 2>/dev/null | grep unixODBC-devel ").read()
            pydevel_dt = os.popen(
                "rpm -qa 2>/dev/null | grep -e 'python\S*-dev' "
            ).read()
            iostat_dt = os.popen("rpm -qa 2>/dev/null | grep -e 'sysstat' ").read()
        else:
            print("OS " + final_name + " " + final_version + " Not supported")
            no_show = 1
except Exception as e:
    pass
# Here Code will check if flags have been set then accordingly lists will be appened with proper data
if nload_dt != "":
    installed.append(nload_dt)
else:
    not_installed.append("Nload")
if vnstat_dt != "":
    installed.append(vnstat_dt)
else:
    not_installed.append("Vnstat")
if gcc_dt != "":
    installed.append(gcc_dt)
else:
    not_installed.append("GCC-C++")
if odbc_dt != "":
    installed.append(odbc_dt)
else:
    not_installed.append("Unix ODBC-Devel")
if sasl_dt != "":
    installed.append(sasl_dt)
else:
    not_installed.append("Cyrus SASL-Devel")
if pydevel_dt != "":
    installed.append(pydevel_dt)
else:
    not_installed.append("Python3-Devel")
if iostat_dt != "":
    installed.append(iostat_dt)
else:
    not_installed.append("Iostat")
installed_string = ":".join(installed)
not_installed_string = ":".join(not_installed)
# Here the code will decide based on the size of list which message to show to user about os packages installation
if no_show == 0:
    if len(installed) == 0:
        print("Cannot proceed as OS packages dependency not satisfied")
    else:
        print("Installed Packages:")
        installed_list = installed_string.split(":")
        installed_listToStr = "".join([str(elem) for elem in installed_list])
        var = ""
        dt_installed = []
        for i in installed_listToStr:
            if i != "\n":
                var = var + i
            else:
                dt_installed.append(var)
                var = ""
        counter = 1
        for i in dt_installed:
            print(counter, "-", i)
            counter = counter + 1
    if len(not_installed) == 0:
        print("Required packages are already Installed")
    else:
        print("Below packages are not Installed:")
        notInstalled_list = not_installed_string.replace(":", "\n")
        notIntalled_listToStr = "".join([str(elem) for elem in notInstalled_list])
        var = ""
        dt_notInstalled = []
        for i in notIntalled_listToStr:
            if i != "\n":
                var = var + i
            else:
                dt_notInstalled.append(var)
                var = ""
        counter = 1
        for i in dt_notInstalled:
            print(counter, "-", i)
            counter = counter + 1
    if (
        "GCC-C++" in not_installed_string
        or "Unix ODBC-Devel" in not_installed_string
        or "Python3-Devel" in not_installed_string
        or "Cyrus SASL-Devel" in not_installed_string
    ):
        text_file = open("hat_file.txt", "r")
        line_list = text_file.readlines()
        listToStr = " ".join([str(elem) for elem in line_list])
        os.popen("rm -rf hat_file.txt").read()
        text_file = open("hat_latest_flag.txt", "w")
        text_file.write("1")
        text_file.close()
else:
    pass
