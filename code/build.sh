#!bin/bash
var=0
sudo python3 package_installer.py
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Error in package_test.py"
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
python3 -m venv $PWD/HDT_ENV/venv
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