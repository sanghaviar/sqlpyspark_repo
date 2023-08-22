from pyspark.sql.functions import expr,col
def pivot_fun(df):
    pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    # pivotDF.show()
    return pivotDF

def unpivoit_fun(pivotDF):
    unpivotexp = "stack(5,'China',China,'INDIA',INDIA,'Swedan',Swedan,'UAE',UAE,'UK',UK) as (Country,Total)"

    unPivotDF = pivotDF.select("Product", expr(unpivotexp)) \
        .where("Total is not null")
    # unPivotDF.show(truncate=False)
    return unPivotDF