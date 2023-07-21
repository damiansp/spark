from numpy import array
from pyspark import SparkContext
from pyspark.mllib.clustering import BisectingKMeans


DATA = '../../data'


def main():
    data = sc.textFile(f'{DATA}/kmeans_data.txt')
    parsed_data = data.map(
        lambda line: array([float(x) for x in line.split()]))
    mod = BisectingKMeans.train(parsed_data, 2, maxIterations=5)
    cost = mod.computeCost(parsed_data)
    print('Cost:', cost)


if __name__ == '__main__':
    sc = SparkContext(appName='k_means')
    main()
    sc.stop()
