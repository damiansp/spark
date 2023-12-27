import re
import numpy as np

from pyspark import SparkConf, SparkContext


conf = SparkConf()
sc = SparkContext(conf=conf)
data = sc.parallelize([
    ('Amber', 22), ('Alfred', 23), ('Skye', 4), ('Albert', 12), ('Amber', 9)])
data_from_file = sc.textFile('../data/VS14MORT.txt.gz', 4)
print(data_from_file.take(1))
data_hetero = sc.parallelize(
    [('Ferrari', 'fast'), {'Porsche': 100_000}, ['Spain', 'visited', 4504]]
).collect()
print(data_hetero[1]['Porsche'])



def extract_info(row):
    selected_indices = [
         2,  4,  5,  6,  7,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22,
        23, 24, 25, 27, 28, 29, 30, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 43,
        44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 58, 60, 61, 62, 63,
        64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82,
        83, 84, 85, 87, 89]
    record_split = re.compile(
        r'([\s]{19})([0-9]{1})([\s]{40})([0-9\s]{2})([0-9\s]{1})([0-9]{1})'
        r'([0-9]{2})([\s]{2})([FM]{1})([0-9]{1})([0-9]{3})([0-9\s]{1})'
        r'([0-9]{2})([0-9]{2})([0-9]{2})([0-9\s]{2})([0-9]{1})([SMWDU]{1})'
        r'([0-9]{1})([\s]{16})([0-9]{4})([YNU]{1})([0-9\s]{1})([BCOU]{1})'
        r'([YNU]{1})([\s]{34})([0-9\s]{1})([0-9\s]{1})([A-Z0-9\s]{4})'
        r'([0-9]{3})([\s]{1})([0-9\s]{3})([0-9\s]{3})([0-9\s]{2})([\s]{1})'
        r'([0-9\s]{2})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})'
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})'
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})'
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})'
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})'
        r'([A-Z0-9\s]{7})([\s]{36})([A-Z0-9\s]{2})([\s]{1})([A-Z0-9\s]{5})'
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})'
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})'
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})'
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})'
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([\s]{1})([0-9\s]{2})'
        r'([0-9\s]{1})([0-9\s]{1})([0-9\s]{1})([0-9\s]{1})([\s]{33})'
        r'([0-9\s]{3})([0-9\s]{1})([0-9\s]{1})')
    try:
        rs = np.array(record_split.split(row))[selected_indices]
    except:
        rs = np.array(['-99'] * len(selected_indices))
    return rs


data_from_file_conv = data_from_file.map(extract_info)
print(data_from_file_conv.map(lambda row: row).take(1))

data_2014 = data_from_file_conv.map(lambda row: int(row[16]))
print(data_2014.take(10))

data_2014_2 = data_from_file_conv.map(lambda row: (int(row[16]), int(row[16])))
print(data_2014_2.take(10))

data_filtered = data_from_file_conv.filter(
    lambda row: row[16] == '2014' and row[21] == 'O')
print(data_filtered.count())

data_2014_flat = data_from_file_conv.flatMap(
    lambda row: (row[16], int(row[16] + 1)))

distinct_gender = data_from_file_conv.map(lambda row: row[5]).distinct()
print(distinct_gender.collect())

frac = 0.1
data_sample = data_from_file_conv(False, frac, 666)  # w/replmnt, frac, seed


rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c', 10)])
rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', 6), ('d', 15)])
rdd3 = rdd1.leftOuterJoin(rdd2)
rdd4 = rd1.join(rdd2)
rdd5 = rdd1.intersection(rdd2)

rdd1 = rdd1.repartition(4)
print(len(rdd1.glom().collect()))


first_row = data_from_file_conv.take(1)
# also .takeSamplie(with_replacement, n_records, seed)

colsum = rdd1.map(lambda row: row[1]).reduce(lambda x, y: x + y)

# Do:
print(data.count())
# Do NOT:
print(len(data.collect()))


print(data.countByKey().items())
