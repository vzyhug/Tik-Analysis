#!/usr/bin/env bash
set -euo pipefail

name_dir=/hadoop/dfs/name
if [[ ! -f "$name_dir/current/VERSION" ]]; then
  if [[ -n "$(find "$name_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "NameNode metadata is nonempty but has no VERSION; refusing to format" >&2
    exit 1
  fi
  hdfs namenode -format -nonInteractive
fi
exec hdfs namenode
