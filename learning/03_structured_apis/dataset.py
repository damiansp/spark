# Dataset is not meaningful in Python, this is just to exemplify
from pyspark.sql imort Row


row = Row(350, True, 'Learning Spark', None)
print(row[0])
print(row[2])
