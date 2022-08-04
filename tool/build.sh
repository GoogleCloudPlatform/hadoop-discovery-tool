#!/bin/bash

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

var=0
python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))' 2> /dev/null | grep -E '3.6|3.7|3.8|3.9'  > /dev/null
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Python version not satisfied"
    exit 1
fi
if [ $var -eq 127 ]
then
    echo "ERROR - Python version not satisfied"
    exit 1
fi
if [ $var -eq 0 ]
then
    echo "*****************************************************************"
    echo "INFO - Python version satisfied"
    echo "*****************************************************************"
fi
echo "Python version "$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')" detected"
python3 --version &>/dev/null
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Error in python environment configurations"
    exit 1
fi
if [ $var -eq 127 ]
then
    echo "ERROR - Python not found"
    exit 1
fi
flag=0
hat_check=0
touch hat_file.txt
echo $hat_check > hat_file.txt
val=$?
if [ $var -eq 127 ]
then
    if [ $val -eq 0 ]
    then
        python3.8 os_package_installer.py
        check=$?
        if [ $check -eq 0 ]
        then
            value="0"
            if [ -e hat_latest_flag.txt ]
            then
                value=$(<hat_latest_flag.txt)
            fi
            rm -rf hat_latest_flag.txt
            if [ $value -eq "1" ]
            then
                echo "ERROR - Cannot proceed as OS Packages dependency not satisfied"
                exit 1
            fi
        fi
        if [ $check -eq 1 ]
        then
            echo "ERROR - Cannot proceed as OS Packages dependency not satisfied"
            exit 1
        fi
        if [ $check -eq 127 ]
        then
            echo "ERROR - Cannot proceed as OS Packages dependency not satisfied"
            exit 1
        fi
    fi
    flag=8
else
    if [ $val -eq 0 ]
    then
        python3 os_package_installer.py
        check=$?
        if [ $check -eq 0 ]
        then
            value="0"
            if [ -e hat_latest_flag.txt ]
            then
                value=$(<hat_latest_flag.txt)
            fi
            rm -rf hat_latest_flag.txt
            if [ $value -eq "1" ]
            then
                echo "ERROR - Cannot proceed as OS Packages dependency not satisfied"
                exit 1
            fi
        fi
        if [ $check -eq 1 ]
        then
            echo "ERROR - Cannot proceed as OS Packages dependency not satisfied"
            exit 1
        fi
        if [ $check -eq 127 ]
        then
            echo "ERROR - Cannot proceed as OS Packages dependency not satisfied"
            exit 1
        fi
    fi
    flag=6
fi
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Packages are not Installed"
    exit 1
fi
if [ $var -eq 0 ]
then
    echo "INFO - OS Dependencies Satisfied "
    echo "###########################################################################################################"
    echo "During python installation, some error messages might pop up , but those can be ignored if last message says,"
    echo "Python Dependencies Installed Successfully"
    echo "############################################################################################################"
fi
# remove old environment directory
rm -rf python_environment
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Old environment not removed - Directory Issue"
    exit 1
fi
# create a new directory for python virtual environment
mkdir python_environment
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Cannot create a new Directory"
    exit 1
fi
if [ $var -eq 0 ]
then
    echo "*****************************************************************"
    echo "INFO - New Directory created for python environment"
fi
if [ $flag -eq 6 ]
then
    python3 python_package_installer.py
fi
if [ $flag -eq 8 ]
then
    python3.8 python_package_installer.py
fi
var=$?
if [ $var -eq 1 ]
then
    echo "*****************************************************************"
    echo "ERROR - Virtual Environment is not created"
    exit 1
fi
if [ $var -eq 0 ]
then
    echo "*****************************************************************"
    echo "INFO - Virtual Environment created successfully"
fi
# Activate python environment
source $PWD/python_environment/venv/bin/activate
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Source not found"
    exit 1
fi
if [ $var -eq 0 ]
then
    echo "*****************************************************************"
    echo "INFO - Python environment activated"
fi
pip3 install pip==21.0.1
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Cannot update pip in python environment"
    exit 1
fi
if [ $var -eq 127 ]
then
    echo "ERROR - pip3 not found"
    exit 1
fi
if [ $var -eq 0 ]
then
echo "*****************************************************************"
echo "INFO - Pip updated in python environment"
fi
pip3 install wheel
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Cannot install wheel in python environment"
    exit 1
fi
if [ $var -eq 0 ]
then
echo "*****************************************************************"
echo "INFO - Wheel installed in python environment"
fi
pip3 install -r $PWD/requirements.txt
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Requirements are not installed completely"
    exit 1
fi
if [ $var -eq 0 ]
then
    echo "*****************************************************************"
    echo "INFO - Python Dependencies Installed Successfully"
fi
# copy codebase into the new virtual environment
cp -a $PWD/codebase/ $PWD/python_environment/
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Cannot copy source into new environment"
    exit 1
fi
if [ $var -eq 0 ]
then
    echo "*****************************************************************"
    echo "INFO - Hadoop Assessment Tool Deployed Successfully"
    echo "*****************************************************************"
    echo "NEXT STEP - Run 'sudo bash run.sh' to generate the PDF report"
    echo "*****************************************************************"
fi
rm -rf hat_file.txt