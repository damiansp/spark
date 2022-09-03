pivoted = df_with_date.groupBy('Date').pivot('Country').sum()
pivoted.where("date > '2011-12-05'").select('Date', '`USA_sum(Quantity)`')
