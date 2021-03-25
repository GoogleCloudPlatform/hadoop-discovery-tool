#!/bin/bash
zkcli=$(find / -path '*/zookeeper/bin/zkCli.sh')
line=$(awk '/zookeeper.connect/'  /etc/kafka/conf/kafka-client.conf)
zookeeper=${line:(18)}
$zkcli  -server  $zookeeper  ls /brokers/ids