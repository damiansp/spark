from pyspark.sql.functions import coalesce


df.select(coalesce(col('description'), col('id'))).show()


'''
SELECT
  ifnull(null, 'return_val') AS a,
  nullif('val', 'val') AS b,
  nvl(null, 'return_val') AS c,
  nvl2('not_null', 'return_val', 'else_val') AS d
FROM dfTable
LIMIT 1'''
#          a |    b |          c |          d
# -----------|------|------------|-----------
# return_val | null | return_val | return_val

df.na.drop()       # any row with na val(s)
df.na.drop('any')  # same
df.na.drop('all')
df.na.drop('all', subset=['code', 'idn'])

df.na.fill('with this value')
df.na.fill('all', subset=['code', 'idn'])
fill_col_vals = {'code': 5, 'idn': 'nonesuch'}
df.na.fill(fill_col_vals)

df.na.replace([''], ['UNKNOWN'], 'description')
