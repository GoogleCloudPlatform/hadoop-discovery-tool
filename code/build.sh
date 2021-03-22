#!bin/bash
var=0
python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))' | grep -E '3.3|3.4|3.5|3.9'  > /dev/null
var=$?
if [ $var -eq 0 ]
then
        echo "Python version not satisfied"
        exit 1
fi
python3 --version 2>/dev/null
var=$?
flag=0
if [ $var -eq 127 ]
then
        python3.8 os_package_installer.py
        flag=8
else
        python3 os_package_installer.py
        flag=6
if [ $var -eq 1 ]
then
		echo "ERROR - Python version error"
		exit 1
fi
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Packages are not Installed"
    exit 1
fi
# remove old environment directory
rm -rf HDT_ENV
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Old environment not removed - Directory Issue"
        exit 1
fi
# create a new directory for python virtual environment
mkdir HDT_ENV
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Cannot create a new Directory"
        exit 1
fi
# create a new python environment
# python3 -m venv $PWD/HDT_ENV/venv
#python3.8 --version 2>/dev/null
#var=$?
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
    echo "ERROR - Virtual Environment is not created"
        exit 1
fi
# Activate python environment
source $PWD/HDT_ENV/venv/bin/activate
var=$?
if [ $var -eq 1 ]
then
    echo "Source not found"
        exit 1
fi
# Copy offline packages into new virtual environment for installation
cp -a $PWD/HDT/packages $PWD/HDT_ENV/packages
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Cannot copy package folder in new environment"
    exit 1
fi
# Copy requirements.txt into the new environments to fetch packages one by one
cp $PWD/HDT/requirements.txt $PWD/HDT_ENV/packages
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Cannot copy requirements in new environment"
    exit 1
fi
# Install all the offline packages into the virtual environment
# pip3 install -r $PWD/HDT_ENV/packages/requirements.txt --no-index --find-links $PWD/HDT_ENV/packages
pip3 install -r $PWD/HDT_ENV/packages/requirements.txt --no-index --no-deps --find-links $PWD/HDT_ENV/packages/
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Requirements are not installed completely"
    exit 1
fi
if [ $var -eq 0 ]
then
    echo "Python Dependencies Installed Successfully"
fi
# copy codebase into the new virtual environment
cp -a $PWD/HDT/ $PWD/HDT_ENV/
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Cannot copy source into new environment"
    exit 1
fi