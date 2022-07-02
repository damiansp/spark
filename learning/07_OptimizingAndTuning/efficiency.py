'''
spark-submit --conf spark.sql.shuffle.partitions=5 \
  --conf "spark.executor.memory=2g" \
  --class main.python.chapter7.SparkConfig_7_1 jars/main-python-chapter7_2.12-1.0 jar
'''

