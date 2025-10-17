#!/bin/bash
MASTER_IP=$(getent hosts spark_master | awk '{ print $1 }')
/opt/spark/sbin/start-worker.sh spark://$MASTER_IP:7077
tail -f /opt/spark/logs/spark*.out