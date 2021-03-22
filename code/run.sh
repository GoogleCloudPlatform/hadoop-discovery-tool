#!bin/bash
var=0
# Activate python environment
source $PWD/HDT_ENV/venv/bin/activate
var=$?
if [ $var -eq 1 ]
then
    echo ""
fi
#Go to the code directory
cd $PWD/HDT_ENV/HDT/ 2>/dev/null
var=$?
if [ $var -eq 1 ]
then
    echo "Directory Issue"
	exit 1
fi
#run python file which will generate the pdf
python3 __main__.py
var=$?
if [ $var -eq 1 ]
then
    echo "Error in Python code check logs for more info"
	exit 1
fi
if [ $var -eq 0 ]
then
	echo "Python Code ran Successfully"
fi
deactivate
cd ..
cd ..