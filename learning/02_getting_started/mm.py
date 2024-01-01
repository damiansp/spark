import sys

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('M&Ms').getOrCreate()


def main(mm_file):
    mm_df = read_data(mm_file)
    agg_df = aggregate(mm_df)
    agg_df.show(n=60, truncate=False)
    print(f'Total Rows: {agg_df.count()}')
    oregon_df = get_state_data(mm_df, 'OR')
    oregon_df.show(n=10, truncate=False)


def read_data(filename):
    df = (
        spark
        .read
        .format('csv')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .load(filename))
    return df


def aggregate(df):
    agg = (
        df
        .select('State', 'Color', 'Count')
        .groupBy('State', 'Color')
        .sum('Count')
        .orderBy('sum(Count)', ascending=False))
    return agg


def get_state_data(df, state):
    state_df = (
        df
        .select('State', 'Color', 'Count')
        .where(df.State == state)
        .groupBy('State', 'Color')
        .sum('Count')
        .orderBy('sum(Count)', ascending=False))
    return state_df        


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: mm <FILE>', file=sys.stderr)
        sys.exit(-1)
    main(sys.argv[1])
    spark.stop()
