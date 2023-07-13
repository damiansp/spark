from pyspark.ml.feature import StringIndexer


def main():
    df = load_and_prep_data()  # omitted
    df encode_categoricals(df)
    

def encode_categorical(df):
    indexer = StringIndexer(inputCol='class', outputCol='label')
    indexed = indexer.fit(df).transform(df)
    indexed.show()
    indexed.select('label').distinct().show()
    return indexed
