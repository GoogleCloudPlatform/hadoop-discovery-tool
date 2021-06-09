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

pyversion = os.popen("python3 -V | awk '{print $2}'").read()
pyversion = pyversion.replace("\n", "")
if pyversion >= "3.8.0":
    os.popen("python3.8 -m venv $PWD/python_environment/venv").read()
else:
    os.popen("python3 -m venv $PWD/python_environment/venv").read()
