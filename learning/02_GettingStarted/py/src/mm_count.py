import sys

from pyspark.sql import SparkSession as SS


def main():
    if len(sys.argv) != 2:
        print('Usage: mm_count <file>', file=sys.stderr)
        sys.exit(-1)

    # Build SparkSession using APIs. If does not exist create. Can only be one
    # SparkSession per JVM
    spark = SS.builder.appName('PythonMMCount').getOrCreate()
    mm_file = sys.argv[1]
    # Read into Spark DF using CSV format
    mm_df = (spark.read.format('csv')
             .option('header', 'true')
             .option('inferSchema', 'true')
             .load(mm_file))
    count_mm_df = (mm_df
                   .select('State', 'Color', 'Count')
                   .groupBy('State', 'Color')
                   .sum('Count')
                   .orderBy('sum(Count)', ascending=False))
    count_mm_df.show(n=50, truncate=False)
    print(f'Total Rows: {count_mm_df.count()}')
    # Show just a single state's data
    ca_count_df = (mm_df
                   .select('State', 'Color', 'Count')
                   .where(mm_df.State == 'CA')
                   .groupBy('State', 'Color')
                   .sum('Count')
                   .orderBy('sum(Count)', ascending=False))
    ca_count_df.show(n=5, truncate=False)
    spark.stop()


if __name__ == '__main__':
    main()
