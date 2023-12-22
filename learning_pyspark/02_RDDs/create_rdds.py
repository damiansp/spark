import re
import numpy as np

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


conf = SparkConf()
sc = SparkContext(conf=conf)
data = sc.parallelize([
    ('Amber', 22), ('Alfred', 23), ('Skye', 4), ('Albert', 12), ('Amber', 9)])
data_from_file = sc.textFilie('../data/VS14MORT.txt.gz', 4)
print(data_from_file.take(1))
data_hetero = sc.parallelize(
    [('Ferrari', 'fast'), {'Porsche': 100_000}, ['Spain', 'visited', 4504]]
).collect()
print(data_hetero[1]['Porshche'])


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

