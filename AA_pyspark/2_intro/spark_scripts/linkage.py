from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, stddev
from pyspark.sql.types import DoubleType


prev = spark.read.csv('linkage')
print(prev.show()) # head

parsed = (spark.read.option('header', 'true')
          .option('nullValue', '?')
          .option('inferSchema', 'true')
          .csv('linkage'))
parsed.printSchema()
parsed.cache()


# Alternatives for known schemata:
'''
from pyspark.sql.types import *
schema = StructType([StructField('id_1', IntegerType(), False),
                     StructField('id_2', StringType(), False),
                     StructField('cmp_fname_c1', DoubleType(), False)])
spark.read.schema(schema).csv('...')
'''

'''
schema = 'id_1 INT, id_2 INT, cmp_fname_c1 DOUBLE'
'''

# Also -- reads/writes
'''
d1 = spark.read.format('json').load('myfile.json')
d2 = spark.read.json('myfile.json')

d1.write.format('parquet').save('myfile.parquet')
d1.write.parquet('myfile.parquet')
d2.write.format('parquet').mode('overwrite').save('myfile.parquet')
'''

print(parsed.first)
parsed.show(5)
n = parsed.count()
print('n:', n)

parsed.groupBy('is_match').count().orderBy(col('count').desc()).show()
parsed.agg(avg('cmp_sex'), stddev('cmp_sex')).show()

parsed.createOrReplaceTempView('linkage') # allow to be treated as a SQL table
spark.sql('''
    SELECT is_match, COUNT(*) n
    FROM linkage
    GROUP BY is_match
    ORDER BY n DESC'''
).show()


# Connect SparkSQL to Hive
#spark_session = (
#    SparkSession.builder.master('local[4]').enableHiveSupport().getOrCreate())



summary = parsed.describe() # count, mean, sd, min, max
summary.show()

matches = parsed.where('is_match=true')
match_summary = matches.describe()
match_summary.show()

misses = parsed.filter(col('is_match') == False)
miss_summary = misses.describe()
miss_summary.show()

summary_p = summary.toPandas()
print(summary_p.head())
print(summary_p.shape)

# Transpose df
summary_p = summary_p.set_index('summary').transpose().reset_index()
summary_p = summary_p.reneame(columns={'index': 'field'})
summary_p = summary_p.rename_axis(None, axis=1)
print(summary_p.shape)

summary_t = spark.createDataFrame(summary_p)
summary_t.show()
summary_t.printSchema()

for c in summary_t.columns:
    if c == 'field':
        continue
    summary_t = summary_t.withColumn(c, summary_t[c].cast(DoubleType()))
summary_t.printSchema()


# Wrap in func
def pivot_summary(desc: DataFrame) -> DataFrame:
    desc_p = desc.toPandas()
    # transpose
    desc_p = desc_p.set_index('summary').transpose.reset_index()
    desc_p = desc_p.reneame(columns={'index': 'field'})
    desc_p = desc_p.rename_axis(None, axis=1)
    # Str -> Double for metrics cols
    for c in desc_t.columns:
    if c == 'field':
        continue
    else:
        desc_t = summary_t.withColumn(c, summary_t[c].cast(DoubleType()))
    return desc_t


match_summary_t = pivot_summary(match_summary)
miss_summary_t = pivot_summary(miss_summary)



# Joins; Feature selection
match_summary_t.createOrReplaceTempView('match_desc')
miss_summary_t.createOrReplaceTempView('miss_desc')
spark.sql(
    '''
    SELECT a.field, a.count + b.count total, a.mean - b.meant delta
    FROM match_desc a
    INNER JOIN miss_desc b ON a.field = b.field
    WHERE a.field NOT IN ('id_1', 'id_2')
    ORDER BY delta DESC, total DESC'''
).show()



# Scoring and Model Eval
good_features = ['cmp_lname_c1', 'cmp_plz', 'cmp_by', 'cmp_bd', 'cmp_bm']
sum_expression = ' + '.join(good_features)
print(sum_expression)

scored = (parsed
          .fillna(0, subset=good_features)
          .withColumn('score', expr(sum_expression))
          .select('score', 'is_match'))
scored.show()


# Confusion matrix
def cross_tabs(scored: DataFrame, t: DoubleType) -> DataFrame:
    return (scored
            .selectExpr(f'score >= {t} as above', 'is_match')
            .groupBy('above')
            .pivot('is_match', ('true', 'false'))
            .count)

crossTabs(scored, 4.).show()
crossTabs(scored, 2.).show()
