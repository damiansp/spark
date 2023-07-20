from pyspark import SparkContext
from pyspark.mllib.linalg import Matrices, Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics


def main():
    vector_example()
    two_way_independence()
    labeled_example()


def vector_example():
    freq_vec = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25)
    chsq_gof = Statistics.chiSqTest(freq_vec)
    print('Hypothesis: all frequencies equally likely:')
    print(chsq_gof)


def two_way_independence():
    contingency_mat = Matrices.dense(3, 2, [1., 3., 5., 2., 4., 6.])
    chsq_res = Statistics.chiSqTest(contingency_mat)
    print('Hypothesis: row fields are independent of column fields:')
    print(chsq_res)


def labeled_example():
    obs = sc.parallelize([
        LabeledPoint(1., [1., 0., 3.]),
        LabeledPoint(1., [1., 2., 0.]),
        LabeledPoint(1., [-1., 0., -0.5])])
    chsq_res = Statistics.chiSqTest(obs)
    print('ChiSquared Results for every feature against the label:')
    for i, res in enumerate(chsq_res):
        print(f'Column {i + 1}:\n{res}\n')
        

if __name__ == '__main__':
    sc = SparkContext(appName='chisq_test')
    main()
    sc.stop()
