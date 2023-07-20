import numpy as np
from pyspark import SparkContext
from pyspark.mllib.stat import Statistics


def main():
    mat = sc.parallelize([
        np.array([1., 10., 100.]),
        np.array([2., 20., 200.]),
        np.array([3., 33., 303.])])
    summary = Statistics.colStats(mat)
    print('means:', summary.mean())
    print('vars:', summary.variance())
    print('N != 0:', summary.numNonzeros())


if __name__ == '__main__':
    sc = SparkContext(appName='summarizer')
    main()
    sc.stop()
