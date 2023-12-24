from pyspark import SparkConf, SparkContext


class MyClass:
    def func(self, s):
        return s

    def do_stuff(self, rdd):
        return rdd.map(self.func)


class MyClass:
    def __init__(self):
        self.field = 'hello'

    def do_stuff(self, rdd):
        # whole self must be sent to cluster
        return rdd.map(lambda s: self.field + s)  

    def do_stuff(self, rdd):
        field = self.field
        # only field is sent to cluster
        return rdd.map(lambda s: field + s)


if __name__ == '__main__':
    conf = SparkConf().setAppName('MyApp')
    sc = SparkContext(conf=conf)

    def count_words(s):
        words = s.split()
        return len(words)

    sc.textFile('file.txt').map(count_words)
