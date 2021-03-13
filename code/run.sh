#!bin/bash
var=0
#Go to the code directory
cd $PWD/HDT_ENV/HDT/ 2>/dev/null
var=$?
if [ $var == 1 ]
then
    echo "Directory Issue"
	break
fi
#run python file which will generate the pdf
python3 __main__.py 2>/dev/null
var=$?
if [ $var == 1 ]
then
    echo "Error in Python code check logs for more info"
	break
fi
if [ $var == 1 ]
then
	echo "Python Code ran Successfully"
fi