"""Diagnostic synthetic data only; never training data or trend evidence."""
import json
import socket
import time
import uuid
from pathlib import Path
from pyspark.sql import SparkSession


def witness_partition(values):
    count = sum(1 for _ in values)
    # Keeps this small diagnostic stage active long enough for both executors.
    time.sleep(0.25)
    yield (socket.gethostname(), count)


def main():
    spark = SparkSession.builder.appName('hdfs-two-worker-smoke').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        expected = {'spark-worker-1', 'spark-worker-2'}
        deadline = time.monotonic() + 60
        registered = set()
        while time.monotonic() < deadline:
            iterator = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keysIterator()
            registered = set()
            while iterator.hasNext():
                registered.add(str(iterator.next()).split(':')[0])
            if expected <= registered:
                break
            time.sleep(1)
        observed = spark.sparkContext.parallelize(range(128), 32).mapPartitions(witness_partition).collect()
        hosts = sorted({host for host, _ in observed})
        path = f'hdfs://namenode:9000/trend/diagnostics/smoke-{uuid.uuid4().hex}'
        spark.range(128, numPartitions=4).write.mode('errorifexists').parquet(path)
        result = spark.read.parquet(path)
        stats = result.selectExpr('count(*) as rows', 'sum(id) as total').first()
        report = {'diagnostic_only': True, 'application_id': spark.sparkContext.applicationId,
                  'master': spark.sparkContext.master, 'executor_hosts_registered': sorted(registered),
                  'worker_hosts_with_tasks': hosts, 'hdfs_path': path,
                  'readback_rows': stats['rows'], 'readback_sum': stats['total']}
        report['passed'] = expected <= set(hosts) and stats['rows'] == 128 and stats['total'] == 8128
        output = Path('/workspace/outputs/quality/spark_hdfs_smoke.json')
        output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(report, indent=2))
        if not report['passed']:
            raise RuntimeError('HDFS/two-worker diagnostic failed; inspect Spark UI and executor logs.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
