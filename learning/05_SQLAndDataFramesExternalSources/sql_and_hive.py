import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# UDFs
def cubed(s):
    return s * s * s


spark.udf.register('cubed', cubed, LongType())
spark.range(1, 9).createOrReplaceTempView('udf_test')
spark.sql('SELECT id, cubed(id) AS id_cubed FROM udf_test').show()


def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

cubed_udf = pandas_udf(cubed, returnType=LongType())

