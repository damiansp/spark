from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import VectorAssembler
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
train_data, test_data = data.randomSplit([0.9, 0.1])
train_data.cache()
test_data.cache()

input_cols = col_names[:-1]
vector_assembler = VectorAssembler(inputCols=input_cols,
                                   outputCol='featureVector')
assembled_train_data = vector_assembler.transform(train_data)
assembled_train_data.select('featureVector').show(truncate=False)

classifier = DecisionTreeClassifier(seed=123, l
                                    abelCol='Cover_Type',
                                    featuresCol='featureVector',
                                    predictionCol='prediction')
model = classifier.fit(assembled_train_data)
print(model.toDebugString)
