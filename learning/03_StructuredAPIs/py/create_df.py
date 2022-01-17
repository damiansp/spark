from pyspark.sql import Row, SparkSession


schema = ('ID INT, First STRING, Last STRING, Url STRING, Published STRING, '
          'Hits INT, Campaigns ARRAY<STRING>')
data = [
    [1, 'Bob', 'Dobolina', 'https://bobdob.com', '1976-11-03', 1234,
     ['web', 'linkedin']],
    [2, 'Max', 'Thenaif', 'https://slasher.com', '1975-01-21', 5678,
     ['linkedin', 'tictoc']],
    [3, 'Abbie', 'Dabbler', 'https://dibdab.com', '1980-03-13', 9012,
     ['tictoc']],
    [4, 'Elmer', 'Glue', 'https://gluesafood.edu', '1982-08-05', 3456,
     ['tictoc', 'fb', 'twtr']]]
row = Row(
    5, 'Whiskers', 'Friskerson', 'https:meowmix4eva.com', '1985-11-07', 7890,
    ['fb'])

rows = [Row('Bob Dobolina', 'NY'), Row('Maude Dobson', 'CA')]


if __name__ == '__main__':
    spork = SparkSession.builder.appName('df').getOrCreate()
    df = spork.createDataFrame(data, schema)
    df.show()
    print(df.printSchema())
    print(row[2])

    df2 = spork.createDataFrame(rows, ['Person', 'Place'])
    df2.show()