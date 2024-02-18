from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


DATA = '../data'
spark = SparkSession.builider.appName('LoadTransform').getOrCreate()
labels = [
    ('INFANT_ALIVE_AT_REPORT', StringType()),
    ('BIRTH_YEAR', IntegerType()),
    ('BIRTH_MONTH', IntegerType()),
    ('BIRTH_PLACE', StringType()),
    ('MOTHER_AGE_YEARS', IntegerType()),
    ('MOTHER_RACE_6CODE', StringType()),
    ('MOTHER_EDUCATION', StringType()),
    ('FATHER_COMBINED_AGE', IntegerType()),
    ('FATHER_EDUCATION', StringType()),
    ('MONTH_PRECARE_RECODE', StringType()),
    ('CIG_BEFORE', IntegerType()),
    ('CIG_1_TRI', IntegerType()),
    ('CIG_2_TRI', IntegerType()),
    ('CIG_3_TRI', IntegerType()),
    ('MOTHER_HEIGHT_IN', IntegerType()),
    ('MOTHER_BMI_RECODE', IntegerType()),
    ('MOTHER_PRE_WEIGHT', IntegerType()),
    ('MOTHER_DELIVERY_WEIGHT', IntegerType()),
    ('MOTHER_WEIGHT_GAIN', IntegerType()),
    ('DIABETES_PRE', StringType()),
    ('DIABETES_GEST', StringType()),
    ('HYP_TENS_PRE', StringType()),
    ('HYP_TENS_GEST', StringType()),
    ('PREV_BIRTH_PRETERM', StringType()),
    ('NO_RISK', StringType()),
    ('NO_INFECTIONS_REPORTED', StringType()),
    ('LABOR_IND', StringType()),
    ('LABOR_AUGM', StringType()),
    ('STEROIDS', StringType()),
    ('ANTIBIOTICS', StringType()),
    ('ANESTHESIA', StringType()),
    ('DELIV_METHOD_RECODE_COMB', StringType()),
    ('ATTENDANT_BIRTH', StringType()),
    ('APGAR_5', IntegerType()),
    ('APGAR_5_RECODE', StringType()),
    ('APGAR_10', IntegerType()),
    ('APGAR_10_RECODE', StringType()),
    ('INFANT_SEX', StringType()),
    ('OBSTETRIC_GESTATION_WEEKS', IntegerType()),
    ('INFANT_WEIGHT_GRAMS', IntegerType()),
    ('INFANT_ASSIST_VENTI', StringType()),
    ('INFANT_ASSIST_VENTI_6HRS', StringType()),
    ('INFANT_NICU_ADMISSION', StringType()),
    ('INFANT_SURFACANT', StringType()),
    ('INFANT_ANTIBIOTICS', StringType()),
    ('INFANT_SEIZURES', StringType()),
    ('INFANT_NO_ABNORMALITIES', StringType()),
    ('INFANT_ANCEPHALY', StringType()),
    ('INFANT_MENINGOMYELOCELE', StringType()),
    ('INFANT_LIMB_REDUCTION', StringType()),
    ('INFANT_DOWN_SYNDROME', StringType()),
    ('INFANT_SUSPECTED_CHROMOSOMAL_DISORDER', StringType()),
    ('INFANT_NO_CONGENITAL_ANOMALIES_CHECKED', StringType()),
    ('INFANT_BREASTFED', StringType())]
schema = typ.StructType([typ.StructField(e[0], e[1], False) for e in labels])
births = spark.read.csv(
    f'{DATA}/births_train.csv.gz', header=True, schema=schema)
recoder = {'YNU': {'Y': 1, 'N': 0, 'U': 0}}

