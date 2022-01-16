import sys

from pyspark.sql import SparkSession


def main():
    print('Python version:', sys.version)
    if len(sys.argv) != 2:
        print('Usage: mm_count [file]', file=sys.stderr)
        sys.exit(-1)

    # Build a SparkSession (can only be one sess per JVM)
    spark = SparkSession.builder.appName('PythonMMCount').getOrCreate()
    mm_file = sys.argv[1]
    mm_df = (spark.read.format('csv')
             .option('header', 'true')
             .option('inferSchema', 'true')
             .load(mm_file))
    count_df = (mm_df.select('State', 'Color', 'Count')
                .groupBy('State', 'Color')
                .sum('Count')
                .orderBy('sum(Count)', ascending=False))
    count_df.show(n=60, truncate=False)
    print(f'Total Rows: {count_df.count()}')

    # Show data just for a single state
    ca_count_df = (mm_df.select('State', 'Color', 'Count')
                   .where(mm_df.State == 'CA')
                   .groupBy('State', 'Color')
                   .sum('Count')
                   .orderBy('sum(Count)', ascending=False))
    ca_count_df.show(n=10, truncate=False)
    spark.stop()
    
    
if __name__ == '__main__':
    main()
