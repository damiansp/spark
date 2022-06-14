from pyspark.sql.functions import (
    expr, initcap, instr, lit, locate, lower, lpad, ltrim, regexp_extract,
    regexp_replace, rpad, rtrim, translate, trim, upper)


df.select(initcap(col('description')))  # like title()
df.select(col('x'), lower(col('x')), upper(col('x')))
df.select(
    ltrim(lit('   hello   ').alias('ltrim')),  # 'hello   '
    rtrim(lit('   hello   ').alias('rtrim')),  # '   hello'
    trim(lit('   hello   ').alias('trim')),    # 'hello'
    lpad(lit('hello'), 3, ' ').alias('lp'),    # ' he'
    rpad(lit('hello'), 7, ' ').alias('rp'))    # 'hello  '

re_str = 'BLACK|WHITE|RED|GREEN|BLUE'
df.select(
    regexp_replace(col('Description'), re_str, 'COLOR')
    .alias('color_clean'), col('Description')
).show()

df.select(
    translate(col('Description'), 'LEET', '1337'), col('Description')
).show()

extr_str = '(BLACK|WHITE|RED|GREEN|BLUE)'
df.select(
    regexp_extract(col('Description'), extract_str, 1)
    .alias('color_clean'),
    col('Description')
).show()

contains_black = instr(col('Description'), 'BLACK') >= 1
contains_white = instr(col('Description'), 'WHITE') >= 1
df.withColumn('hasSimpleColor', contains_black | contains_white).where('hasSimpleColor').select('Description').show(3, False)
                       
simple_colors = ['red', 'green', 'blue', 'black', 'white']


def locate_color(column, color_str):
    return locate(color_str.upper(), column).cast('boolean').alias(f'is_{c}')


selected_cols = [locate_color(df.Description, c) for c in simple_colors]
selected_cols.append(expr('*'))  # make Column type

(df
 .select(*selected_cols)
 .where(expr('is_white OR is_red'))
 .select('Description')
 .show(5, False))
