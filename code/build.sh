#!bin/bash
python3 package_test.py &&
# remove old environment directory
rm -rf HDT_ENV &&
# create a new directory for python virtual environment
mkdir HDT_ENV &&
# create a new python environment
python3 -m venv $PWD/HDT_ENV/venv &&
# Activate python environment
source $PWD/HDT_ENV/venv/bin/activate &&
# Copy offline packages into new virtual environment for installation
cp -a $PWD/HDT/packages $PWD/HDT_ENV/packages &&
# Copy requirements.txt into the new environments to fetch packages one by one
cp $PWD/HDT/requirements.txt $PWD/HDT_ENV/packages &&
# Install all the offline packages into the virtual environment
sudo pip3 install -r $PWD/HDT_ENV/packages/requirements.txt --no-index --find-links $PWD/HDT_ENV/packages &&
echo "Python Dependencies Installed Successfully" &&
# copy codebase into the new virtual environment
cp -a $PWD/HDT/ $PWD/HDT_ENV/ &&

