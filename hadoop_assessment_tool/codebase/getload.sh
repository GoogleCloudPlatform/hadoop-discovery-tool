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

cd /sys/class/net/$(route | grep default | awk '{print $NF}')/statistics/
end=$((SECONDS+10));
old_rx="$(<//sys/class/net/$(route | grep default | awk '{print $NF}')/statistics/rx_bytes)"
old_tx="$(</sys/class/net/$(route | grep default | awk '{print $NF}')/statistics/tx_bytes)"
while $(sleep 1); do
        if [ $SECONDS -lt $end ]; then
                now_rx=$(<rx_bytes);
                now_tx=$(<tx_bytes);
                echo 1,$(($(expr "${now_rx}" - "${old_rx}"))),$(($(expr "${now_tx}" - "${old_tx}")));
                old_rx=$now_rx;
                old_tx=$now_tx;
        else
                break
        fi
done