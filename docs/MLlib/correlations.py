import sys

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.mllib.util import MLUtils


DATA = '../../data'
CORR_METHOD = 'pearson'


def main(inpath):
    points = (
        MLUtils
        .loadLibSVMFile(sc, inpath)
        .map(lambda lp: LabeledPoint(lp.label, lp.features.toArray())))
    print('Summary of data file:', inpath)
    print(f'{points.count()} data points\n')
    print(f'Corr ({CORR_METHOD}) between label and each feature\n')
    print('Feature\tCorrelation')
    n_features = points.take(1)[0].features.size
    label_rdd = points.map(lambda lp: lp.label)
    for i in range(n_features):
        feature_rdd = points.map(lambda lp: lp.features[i])
        corr = Statistics.corr(label_rdd, feature_rdd, CORR_METHOD)
        print(f'{i}\t{corr:g}')

        
if __name__ == '__main__':
    if len(sys.argv) not in [1, 2]:
        print('Usage: correlations <FILE>', file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName='correlations')
    filepath = (
        sys.argv[1] if len(sys.argv) == 2
        else f'{DATA}/sample_linear_regression_data.txt')
    main(filepath)
    sc.stop()


# Derived from: https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/correlations.py
# Orig. license:
#
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
#
