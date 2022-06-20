from pyspark.sql.functions import (
    from_json, get_json_object, json_tuple, to_json)
from pyspark.sql.types import StringType, StructField, StructType


json_df = spark.range(1).selectExpr(
    '''
    '{"my_key": {"my_value": [1, 2, 3]}}' AS json_str''')
json_df.select(
    get_json_object(col('json_str'), '$.my_key.my_value[1]') as 'column',
    json_tuple(col('json_str'), 'my_key')
).show(2)

df.selectExpr('(field1, field2) AS my_struct').select(to_json('my_struct'))

parse_schema = StructType((
    StructField('field1', StringType(), True),
    StructField('field', StringType(), True)))
(df
 .selectExpr('(field1, field2) AS my_struct')
 .select(to_json(col('my_struct')).alias('new_json'))
 .select(from_json(col('new_json'), parse_schema), col('new_json'))
 .show(2))
