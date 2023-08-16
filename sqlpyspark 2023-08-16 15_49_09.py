# Databricks notebook source
data = [(1,'maher','male',20000,'IT'),
        (2,'john','male',30000,'HR'),
        (3,'sara','female',40000,'Payroll'),
        (4,'liz','female',20000,'HR'),
        (5,'Nancy','female',80000,'IT'),
        (6,'Saraf','male',20000,'IT')]
schema = ['id','name','gender','salary','dep']
df = spark.createDataFrame(data,schema)
# df.show()
# For each dept what is count of employee
df1 = df.groupby('dep').count()
# df1.show()
df2 = df.groupby('name','salary').max('salary')
# df3 = df2.select((col('salary')).alias('empSal'))
# df3.show()
df4 = df.groupby('gender','dep').count()
df4.show()
