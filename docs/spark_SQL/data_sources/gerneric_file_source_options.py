# Assumed dir str:
# dir1/
#  L dir2/
#  |  L file2.parquet (cont: "file2.parquet")
#  L file1.parquet    (cont: "file1.parquet")
#  L file3.json       (cont: "{'file': 'corrupt.json'}")

# Ignore corrupt files
test_corrupt_df0 = (
    spark
    .read
    .option('ignoreCorruptFiles', 'true')
    .parquet('dir1/', 'dir1/dir2/'))

# Glob
df = spark.read.load('dir1', format='parquet', pathGlobFilter='*.parquet')


# Recursive file lookup
df = (
    spark
    .read
    .format('parquet')
    .option('recursiveFileLookup', 'true')
    .load('dir1'))


# Load based on modification time
df = spark.load('dir1', format='parquet', modifiedBefore='2050-07-01T08:30:00')
df = spark.load('dir1', format='parquet', modifiedAfter='1976-11-03T01:23:00')
                
