from pyspark.sql.functions import first,col,max,min,avg,sum,desc
def first_row(df):
    # Select first row from each department group.
    df1 = df.groupby('department').agg(first('employee_name').alias('employee_name'), first('salary').alias('salary'))
    return df1

def highest_salary(df):
    # Employee who earns highest salary
    df2 = df.orderBy(col('salary').desc()).limit(1)
    return df2

def agg_fun(df,df1,df2):
    # Select the highest, lowest, average, and total salary for each department group.
    df3 = df.groupBy('department').agg(max('salary').alias('Maximum Salary'))
    df4 = df.groupby('department').agg(min('salary').alias('Minimum Salary'))
    df5 = df.groupby('department').agg(avg('salary').alias('Average salary'))
    df6 = df.groupby('department').agg(sum('salary').alias('Total salary'))

    combined_df = df1.join(df2, on='department', how="outer") \
        .join(df3, on="department", how="outer") \
        .join(df4, on="department", how="outer") \
        .join(df5, on="department", how='outer') \
        .join(df6, on="department", how='outer')
    # combined_df.show(truncate = False)
    return combined_df



