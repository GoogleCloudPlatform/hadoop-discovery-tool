#!bin/bash
var=0
python3 package_test.py
var=$?
if [ $var == 1 ]
then
    echo "ERROR - Error in package_test.py"
        break
fi
# remove old environment directory
rm -rf HDT_ENV
var=$?
if [ $var == 1 ]
then
    echo "ERROR - Old environment not removed - Directory Issue"
	break
fi
# create a new directory for python virtual environment
mkdir HDT_ENV
var=$?
if [ $var == 1 ]
then
    echo "ERROR - Cannot create a new Directory"
	break
fi
# create a new python environment
python3 -m venv $PWD/HDT_ENV/venv
var=$?
if [ $var == 1 ]
then
    echo "ERROR - Virtual Environment is not created"
	break
fi
# Activate python environment
source $PWD/HDT_ENV/venv/bin/activate
var=$?
if [ $var == 1 ]
then
    echo ""
fi
# Copy offline packages into new virtual environment for installation
cp -a $PWD/HDT/packages $PWD/HDT_ENV/packages
var=$?
if [ $var == 1 ]
then
    echo "ERROR - Cannot copy package folder in new environment"
    break
fi
# Copy requirements.txt into the new environments to fetch packages one by one
cp $PWD/HDT/requirements.txt $PWD/HDT_ENV/packages
var=$?
if [ $var == 1 ]
then
    echo "ERROR - Cannot copy requirements in new environment"
    break
fi
# Install all the offline packages into the virtual environment
sudo pip3 install -r $PWD/HDT_ENV/packages/requirements.txt --no-index --find-links $PWD/HDT_ENV/packages
var=$?
if [ $var == 1 ]
then
    echo "ERROR - Requirements are not installed completely"
    break
fi
if [ $var == 0 ]
then
    echo "Python Dependencies Installed Successfully"
fi
# copy codebase into the new virtual environment
cp -a $PWD/HDT/ $PWD/HDT_ENV/
var=$?
if [ $var == 1 ]
then
    echo "ERROR - Cannot copy source into new environment"
    break
fi