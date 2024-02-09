#import bokeh.charts as chrt
#from bokeh.io import output_notebook
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType


plt.style.use('ggplot')


DATA = '../data'

spark = SparkSession.builder.appName('Exploration').getOrCreate()
sc = spark.sparkContext


fraud = sc.textFile(f'{DATA}/ccFraud.csv.gz')
header = fraud.first()
fraud = (
    fraud
    .filter(lambda row: row != header)
    .map(lambda row: [int(e) for e in row.split(',')]))
fields = [
    *[StructField(h[1:-1], IntegerType(), True) for h in header.split(',')]]
schema = StructType(fields)
fraud_df = spark.createDataFrame(fraud, schema)


# Histograms
hists = fraud_df.select('balance').rdd.flatMap(lambda row: row).histogram(20)
data = {'bins': hists[0][:-1], 'freq': hists[1]}
plt.bar(data['bins'], data['freq'], width=2000)
plt.title('Balance')
plt.show()


# Scatterplot
numeric = ['balance', 'numTrans', 'numIntlTrans']
sample = fraud_df.sample_by('gender', {1: 0.0002, 2: 0.0002}).select(numeric)
data_multi = dict([
    (elem, sample.select(elem).rdd.flatMap(lambda row: row).collect())
    for elem in numeric])
#scat = chrt.Scatter(data_multi, x='balance', y='numTrans')
#chrt.show(scat)
