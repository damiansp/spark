# Derived from
# https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/binary_classification_metrics_example.py

# Original license:
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
from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.util import MLUtils


DATA = '../../data'


def main():
    data = MLUtils.loadLibSVMFile(
        sc, f'{DATA}/sample_binary_classification_data.txt')
    train, test = data.randomSplit([0.7, 0.3], seed=42)
    train.cache()
    mod = LogisticRegressionWithLBFGS.train(train)
    preds_and_labels = test.map(
        lambda record: (float(mod.predict(record.features)), record.label))
    metrics = BinaryClassificationMetrics(preds_and_labels)
    auc_pr = metrics.areaUnderPR
    auc_roc = metrics.areaUnderROC
    print('Area under Prec/Rec curve:', auc_pr)
    print('Area under ROC curve:', auc_roc)
    

if __name__ == '__main__':
    sc = SparkContext(appName='binary_clf_metrics')
    main()
    sc.stop()
