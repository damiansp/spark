from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth


def main():
    data = sc.textFile('data/mllib/sample_fpgrowth.txt')
    transactions = data.map(lambda line: line.strip().split())
    mod = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
    res = mod.freqItemsets().collect()
    for r in res:
        print(r)


if __name__ == '__main__':
    sc = SparkContext(appName='fp_growth')
    main()
    sc.stop()


# Derived from: https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/fpgrowth_example.py
# Orig license:
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
