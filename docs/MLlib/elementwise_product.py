from pyspark import SparkContext
from pyspark.mllib.feature import ElementwiseProduct
from pyspark.mllib.linalg import Vectors


def main():
    data = sc.textfile('data/mllib/kmeans_data.txt')
    parsed = data.map(lambda x: [float(t) for t in x.split()])
    transforming_vec = Vectors.dense([0., 1., 2.])
    tranformer = ElementwiseProduct(transforming_vec)
    transformed = transformer.transform(parsed)
    transformed2 = transformer.transform(parsed.first())
    print('Transformed:')
    for x in transformed.collect():
        print(x)
    print('Transformed 2:')
    for x in transformed2.collect():
        print(x)
        

if __name__ == '__main__':
    sc = SparkContext(appName='elemwise_product')
    main()
    sc.stop()

# Derived from: https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/elementwise_product_example.py
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
