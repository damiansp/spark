from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType


DATA = '../../data'


data_sans_header = (spark.read.option('inferSchem', True)
                    .option('header', False)
                    .csv(f'{DATA}/covtype.data'))
data_sans_header.printSchema()

col_names = (
    ['Elevation', 'Aspect', 'Slope', 'Horizontal_Distance_To_Hydrology',
     'Vertical_Distane_To_Hydrology', 'Horizontal_Distance_To_Roadways',
     'Hillshade_9am', 'Hillshade_Noon', 'Hillshade_3pm',
     'Horizontal_Distance_To_Fire_Points']
    + [f'Wilderness_Area_{i}' for i in range(4)]
    + [f'Soil_Type_{i}' for i in range(40)]
    + ['Cover_Type'])
data = (data_sans_header.toDF(*col_names)
        .withColumn('Cover_Type', col('Cover_Type').cast(DoubleType())))
print(data.head())


# First Tree
             
