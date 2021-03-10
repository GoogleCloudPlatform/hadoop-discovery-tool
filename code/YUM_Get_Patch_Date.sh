#!/bin/bash
grep "$(yum updateinfo list security installed | tail -n +3 | head -n -1 | awk '{ print $3 }')" <(rpm -qa --last) > patch_date.csv
