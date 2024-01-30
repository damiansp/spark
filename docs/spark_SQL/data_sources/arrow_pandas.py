from pyspark.sql import SparkSession

import numpy as np
import pandas as pd


spark = SparkSession.builder.appName('arrow_pandas').getOrCreate()


# Convert to/from Pandas
spark.conf.set('spark.sql.exectution.arrow.pyspark.enabled', 'true')
pdf = pd.DataFrame(np.random.rand(100, 3))
df = spark.createDataFrame(pdf)
res_pdf = df.select('*').toPandas()
print(res_pdf.head())
