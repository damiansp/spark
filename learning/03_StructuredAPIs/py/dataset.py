from pyspark.sql import Row


row = Row(350, True, 'Sparkly', None)

print(row[0]) # 350
print(row[1]) # True ...
