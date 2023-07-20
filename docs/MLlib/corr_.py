import numpy as np
from pyspark import SparkContext
from pyspark.mllib.stat import Statistics


def main():
    x, y, rdd = init_data()
    corr_xy = Statistics.corr(x, y)  # method = 'pearson' by default
    print(f'corr(x, y): {corr_xy}')
    corr_rdd = Statistics.corr(rdd)
    corr_rdd_spearman = Statistics.corr(rdd, method='spearman')
    print('RDD correlations:')
    print('Pearson:')
    print(corr_rdd)
    print('\nSpearman:')
    print(corr_rdd_spearman)


def init_data():
    x = sc.parallelize([1., 2., 3., 3., 5.])
    y = sc.parallelize([11., 22., 33., 44., 5.])
    rdd = sc.parallelize([
        np.array([1., 10., 100.]),
        np.array([2., 22., 202.]),
        np.array([5., 33., 366.])])
    return x, y, rdd

if __name__ == '__main__':
    sc = SparkContext(appName='CorrEx')
    main()
    sc.stop()
