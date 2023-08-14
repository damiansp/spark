from pyspark import SparkContext
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils


DATA = '../data'


def main():
    data = MLUtils.loadLibSVMFile(sc, f'{DATA}/sample_libsvm_data.txt')
    train, test = data.randomSplit([0.7, 0.3])
    mod = DecisionTree.trainRegressor(
        train,
        categoricalFeaturesInfo={},
        impurity='variance',
        maxDepth=5,
        maxBins=32)
    preds = mod.predicts(test.map(lambda x: x.features))
    labels_and_preds = test.map(lambda lp: lp.label).zip(preds)
    test_mse = (
        labels_and_preds.map(lambda lp: (lp[0] - lp[1])**2).sum()
        / float(test.count()))
    print(f'Test MSE: {test_mse}')
    print('Model:')
    print(mod.toDebugString())
    mod.save(sc, 'DecisionTreeRegressionMod')
    remod = DecisionTreeModel.load(sc, 'DecisionTreeRegressionMod')
                         

if __name__ == '__main__':
    sc = SparkContext(appName='decision_tree_regressor')
    main()
    sc.stop()

# Derived from
# https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/decision_tree_regression_example.py

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
