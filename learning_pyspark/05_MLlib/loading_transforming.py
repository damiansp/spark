import numpy as np
from pyspark.mllib.classification import (
    LogisticRegressionWithLBFGS as LogisticRegression)
import pyspark.mllib.evaluation as ev
import pyspark.mllib.features as ft
import pyspark.mllib.linalg as la
import pyspark.mllib.regression as reg
import pyspark.mllib.stat as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


DATA = '../data'
spark = SparkSession.builder.appName('LoadTransform').getOrCreate()
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
schema = StructType([StructField(e[0], e[1], False) for e in labels])
births = spark.read.csv(
    f'{DATA}/births_train.csv.gz', header=True, schema=schema)
recoder = {'YNU': {'Y': 1, 'N': 0, 'U': 0}}
use = (
    'infant_alive_at_report birth_place mother_age_years father_combined_age '
    'cig_before cig_1_tri cig_2_tri cig_3_tri mother_height_in '
    'mother_pre_weight mother_weight_gain diabetes_pre diabetes_gest '
    'hyp_tens_pre hyp_tens_gest prev_birth_preterm'
).upper().split()
births_trimmed = births.select(use)


def recode(field, key):
    return recoder[key][field]


recode_int = udf(recode, IntegerType())


def correct_cig(field):
    return when(col(field) != 99, col(field)).otherwise(0)


births_trans = (
    births_trimmed
    .withColumn('CIG_BEFORE', correct_cig('CIG_BEFORE'))
    .withColumn('CIG_1_TRI', correct_cig('CIG_1_TRI'))
    .withColumn('CIG_2_TRI', correct_cig('CIG_2_TRI'))
    .withColumn('CIG_3_TRI', correct_cig('CIG_3_TRI')))
cols = [(col.name, col.dataType) for col in births_trimmed.schema]
YNU_cols = []
for i, s in enumerate(cols):
    if s[1] == StringType():
        dist = (
            births
            .select(s[0])
            .distinct()
            .rdd.map(lambda row: row[0])
            .collect())
        if 'Y' in dist:
            YNU_cols.append(s[0])
INA = 'INFANT_NICU_ADMISSION'
births.select([INA, recode(INA, lit('YNU')).alias(f'{INA}_RECODE')]).take(5)
exprs_YNU = [
    recode(x, lit('YNU')).alias(x) if x in YNU_cols else x
    for x in births_trans.columns]
births_trans = births_trans.select(exprs_YNU)
births_trans.select(YNU_cols[-5:]).show(5)


# Descriptive Stats
numerics = [
    'MOTHER_AGE_YEARS', 'FATHER_COMBINED_AGE', 'CIG_BEFORE', 'CIG_1_TRI',
    'CIG_2_TRI', 'CIG_3_TRI', 'MOTHER_HEIGHT_IN', 'MOTHER_PRE_WEIGHT',
    'MOTHER_DELIVERY_WEIGHT', 'MOTHER_WEIGHT_GAIN']
numeric_rdd = (
    births_trans.select(numerics).rdd.map(lambda row: [e for e in row]))
stats = st.Statistics.colStats(numeric_rdd)
for col, mu, sig in zip(numerics, stats.mean(), stats.variance()):
    print(f'{col}:\t{mu:.2f}\t{sig:.sf}')

categoricals = [e for e in births_trans.columns if e not in numerics]
categorical_rdd = (
    births_trans.select(categorical).rdd.map(lambda row: [e for e in row]))
for i, col in enumerate(categoricals):
    agg = (
        categorical_rdd
        .groupBy(lambda row: row[i])
        .map(lambda row: (row[0], len(row[1]))))
    print(col, sorted(agg.collect(), key=lambda e: e[1], reverse=True))


# Correlations
corrs = st.Statistics.corr(numeric_rdd)
for i, el in enumerat(corrs > 0.5):
    correlated = [
        (numerics[j], corrs[i][j]) for j, e in enumerate(el)
        if e == 1. and j!= i]
    if correlated:
        for e in correlated:
            print(f'{numerics[i]}-to{e[0]}: {e[1]:.2f}')

keep = [
    'INFANT_ALIVE_AT_REPORT', 'BIRTH_PLACE', 'MOTHER_AGE_YEARS',
    'FATHER_COMBINED_AGE', 'CIG_1_TRI', 'MOTHER_HEIGHT_IN',
    'MOTHER_PRE_WEIGHT', 'DIABETES_PRE', 'DIABETES_GEST', 'HYP_TENS_PRE',
    'HYP_TENS_GEST', 'PREV_BIRTH_PRETERM']
births_trans = births_trans.select(keep)  # *keep?


# Statistical testing
for cat in categoricals[1:]:
    agg = births_trans.groupby('INFANT_ALIVE_AT_REPORT').pivot(cat).count()
    agg_rdd = (
        agg.rdd
        .map(lambda row: row[1:])
        .flatMap(lambda row: [0 if e is None else e for e in row])
        .collect())
    row_len = len(agg.collect()[0]) - 1
    agg = la.Matrices.dense(row_len, 2, agg_rdd)
    test = st.Statistics.chiSqTest(agg)
    print(cat, round(test.pValue, 4))


# RDD of labeled points
hashing = ft.HashingTF(7)
births_hashed = (
    births_trans
    .rdd
    .map(
        lambda row: [
            list(hashing.transform(row[1]).toArray()) if col == 'BIRTH_PLACE'
            else row[i]
            for i, col in enumerate(keep)])
    .map(lambda row: [[e] if type(e) == int else e for e in row])
    .map(lambda row: [item for sublist in rwo for item in sublist])
    .map(lambda row: reg.LabeledPoint(row[0], la.Vectors.dense(row[1:]))))


# Train-Test split
births_train, births_test = births_hashed.randomSplit([0.6, 0.4])


# Logistic Regression
lr_mod = LogisticRegression.train(births_strain, iterations=10)
lr_res = (
    births_test
    .map(lambda row: row.label)
    .zip(lr_mod.predict(births_test.map(lambda row: row.features)))
).map(lambda row: (row[0], row[1] * 1.))
lr_eval = ev.BinaryClassificationMetrics(lr_res)
print(f'Area under PR: {lr_eval.areaUnderPR:.2f}')  # Precision-Recall curve
print(f'Area under ROC: {lr_eval.areaUnderROC:.2f}')
lr_eval.unpersist()
