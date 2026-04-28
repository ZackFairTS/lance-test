#!/bin/bash
set -e

if [ -n "$FLINK_PROPERTIES" ]; then
  echo "$FLINK_PROPERTIES" >> $FLINK_HOME/conf/flink-conf.yaml
fi

if [ "$1" = "jobmanager" ]; then
  exec gosu flink $FLINK_HOME/bin/jobmanager.sh start-foreground
elif [ "$1" = "taskmanager" ]; then
  exec gosu flink $FLINK_HOME/bin/taskmanager.sh start-foreground
elif [ "$1" = "compactor" ]; then
  shift
  exec python3 /workspace/scripts/compactor.py "$@"
elif [ "$1" = "sleep" ]; then
  sleep infinity
else
  exec "$@"
fi
