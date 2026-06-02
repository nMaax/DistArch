import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_path = 'users.txt'
products_path = 'products.txt'
purchases_path = 'purchases.txt'

# ------------------------------------
# Part 1

users_df = spark.read.csv(users_path, header=True, inferSchema=True)
products_df = spark.read.csv(products_path, header=True, inferSchema=True)
purchases_df = spark.read.csv(purchases_path, header=True, inferSchema=True)

# Select only what necessary
# I will re-use this df later
users_df = users_df.filter("Country == 'Italian'").select("UserID", "Type")

# NOTE: probably SQL already has some built-in function to get the year out of a string, parsing it via ISO etc.
# but I cannot remind it, so I will define a UDF. If one had access to full documentation then they should prefer the
# built-in function for optimization
def year(purchaseDate):
    return int(purchaseDate[:4])
spark.udf().register("year", year)

# Select only what necessary
purchases_2010_2020 = (
    purchases_df
    .filter("year(purchaseDate) >= 2010 AND year(purchaseDatee) <= 2020")
    .select("PurchaseID", "UserID")
    )

# We dont want non-italian users, nor italian users who only purchased outside the 2010, 2020 range
# since PurchaseID is the primary key, count() on
purchases_per_type = (
    purchases_2010_2020
    .join(users_df, on="UserID", how="inner")
    .groupBy("Type")
    .count() # Could have also used agg("PurchaseID", "count AS Purchases")
    .withColumnRenamed("count", "Purchases") # I assume spark will name the count() it "count(1)"
)

# Save the result
purchases_per_type.write.csv(output_dir1)

# ------------------------------------ # Part 2

users_df = users_df.select("UserID") # We dont need Type anymore

purchases_from_2024 = (
    purchases_df
    .filter("year(purchaseDate) >= 2024")
    .select("PurchaseID", "UserID")
    )

# NOTE: maybe I could have used the IN () clause by using the spark.sql() command or equivalent
# then I may had need to register the dataframes as views to be visible to sql
inactive_users_from_2024 = (
    users_df
    # Spark provides left semi and left anti, both will return only columns from the left df, semi will select such rows that have a matching key in the right, anti will select only rows from the left that have no matching key on the right
    .join(purchases_from_2024, on="UserID", how="left_anti")
)

purchases_in_2023 = (
    purchases_df
    .filter("year(purchaseDate) = 2023")
    .select("PurchaseID", "UserID")
    )

inactive_users_purchases_in_2023 = (
    purchases_in_2023
    .join(inactive_users_from_2024, on="UserID", how="right")
    .groupBy("UserID")
    # we cannot just .count() as it would count rows with NaN PurchaseID as if they where 1 item,
    # if we instead call .count(columnName) we force the system to only count items with non-null values in that specific column \
    # i.e., the below purchaseID will automatically ignore None items returned from the right outer join above
    # as a result, we dont even have to care about using .fillna(0) for such items, count(PurchaseID) will automatically count such users who did not buy anything in 2023 as 0
    # however, count("column") is not available in spark! So we must use agg
    .agg{"PurchaseID": "count"} # or agg(count("PurchaseID").alias("Purchases")) or .agg(expr("count(PurchaseID) AS Purchases"))
    .withColumnRenamed("count(PurchaseID)", "Purchases")
    )

# Save result
inactive_users_purchases_in_2023.write.csv(output_dir2)



















