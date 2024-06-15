import pyspark.ml.feature as ft
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


labels = [
    ('INFANT_ALIVE_AT_REPORT', IntegerType()),
    ('BIRTH_PLACE', StringType()),
    ('MOTHER_AGE_YEARS', IntegerType()),
    ('FATHER_COMBINED_AGE', IntegerType()),
    ('CIG_BEFORE', IntegerType()),
    ('CIG_1_TRI', IntegerType()),
    ('CIG_2_TRI', IntegerType()),
    ('CIG_3_TRI', IntegerType()),
    ('MOTHER_HEIGHT_IN', IntegerType()),
    ('MOTHER_PRE_WEIGHT', IntegerType()),
    ('MOTHER_DELIVERY_WEIGHT', IntegerType()),
    ('MOTHER_WEIGHT_GAIN', IntegerType()),
    ('DIABETES_PRE', IntegerType()),
    ('DIABETES_GEST', IntegerType()),
    ('HYP_TENS_PRE', IntegerType()),
    ('HYP_TENS_GEST', IntegerType()),
    ('PREV_BIRTH_PRETERM', IntegerType())]
schema = StructType([StructField(e[0], e[1], False) for e in labels])
births = spark.read.csv(
    f'{DATA}/births_transformed.csv.gz', header=True, schema=schema)
