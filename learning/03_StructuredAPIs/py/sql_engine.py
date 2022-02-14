count_mnm_df = (
    mnm_df
    .select('State', 'Color', 'Count')
    .groupBy('State', 'Color')
    .agg(count('Count').alias('Total'))
    .orderBy('Total', ascending=False))
