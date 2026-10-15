#!/usr/bin/env bash
set -euo pipefail
export PATH="/opt/hadoop/bin:$PATH"
mkdir -p /tmp/hadoop-logs /tmp/hadoop
case "${1:-}" in
  namenode)
    mkdir -p /data/name
    if [[ ! -f /data/name/current/VERSION ]]; then
      if [[ -n "$(find /data/name -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
        echo 'NameNode volume is nonempty but VERSION is missing; refusing to format.' >&2
        exit 1
      fi
      hdfs namenode -format -nonInteractive -clusterid trend-detect-day1
    fi
    exec hdfs namenode
    ;;
  datanode)
    mkdir -p /data/data
    exec hdfs datanode
    ;;
  *) echo 'Expected namenode or datanode.' >&2; exit 2 ;;
esac
