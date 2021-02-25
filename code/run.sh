# ------------------------------------------------------------------------------
# This shell script is the driver program of the tool. This script will install 
# OS related packages by calling package_test.py and will also install the 
# required python packages and then create a virtual environment required for 
# the execution of the tool. 
# ------------------------------------------------------------------------------


# package_test.py will install os packages which are essential for the script
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
pip3 install -r $PWD/HDT_ENV/packages/requirements.txt --no-index --find-links $PWD/HDT_ENV/packages &&
echo "Python Dependencies Installed Successfully" &&
# copy codebase into the new virtual environment
cp -a $PWD/HDT/ $PWD/HDT_ENV/ &&
cd $PWD/HDT_ENV/HDT/ &&
# run the main python file which will generate the pdf
python3 __main__.py &&
echo "Completed"
