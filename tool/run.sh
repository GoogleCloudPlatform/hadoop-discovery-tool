#!bin/bash
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
# Activate python environment
source $PWD/python_environment/venv/bin/activate
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Unable to activate python environment"
    exit 1
fi
#Go to the code directory
cd $PWD/python_environment/codebase/ 2>/dev/null
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Directory Issue"
	exit 1
fi
#run python file which will generate the pdf
python3 __main__.py 2>../../hadoop_assessment_tool_terminal.log
var=$?
if [ $var -eq 1 ]
then
    echo "ERROR - Unable to run Hadoop Assessment tool, check logs for more info"
	exit 1
fi
deactivate
cd ../..