person = spark.createDataFrame([
    (0, 'Bill Chambers', 0, [100]),
    (1, 'Matei Zaharia', 1, [500, 250, 100]),
    (2, 'Michael Armbrust', 1, [250, 100])
]).toDF('id', 'name', 'grad_program', 'spark_status')
grad_program = spark.createDataFrame([
    (0, 'MS', 'School of Information', 'UC Berkeley'),
    (1, 'PhD', 'EECS', 'UC Berkeley'),
    (2, 'MS', 'EECS', 'UC Berkeley')
]).toDF('id', 'degree', 'dept', 'school')
spark_status = spark.createDateFrame([
    (100, 'contributor'),
    (250, 'PMC member'),
    (500, 'VP')
]).toDF('id', 'status')
person.createOrReplaceTempView('person')
grad_program.createOrReplaceTempView('grad_program')
spark_status.createOrReplaceTempView('spark_status')


# inner joins
join_expr = person['graduate_program'] == grad_program['id']
person.join(grad_program, join_expr, 'inner')


# outer
person.join(grad_program, join_expr, 'outer')
person.join(grad_program, join_expr, 'left_outer')
person.join(grad_program, join_expr, 'right_outer')


# semi
# keep all rows in left if in right (doesn't actually join/return any data from
# right)
person.join(grad_program, join_expr, 'left_semi')

