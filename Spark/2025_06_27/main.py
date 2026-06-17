import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_path = 'users.txt'
products_path = 'products.txt'
purchases_path = 'purchases.txt'

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"

# ------------------------------------
# Part 1

"""
For each user type, total number of purchases from 2010 to 2020 made by Italian
users. The first part of this application considers only Italian users and the
purchases from 2010 to 2020. It computes, for each user type, the total number of
purchases made by the Italian users from 2010 to 2020. Store the result in the first
output folder. The output contains one line for each user type.
The output format is as follows:
user type, total number of purchases made by the Italian users of this user type
from 2010 to 2020.
"""

users = spark.read.csv(users_path, header=True, inferSchema=True) # (UserID,Age,Gender,Country,UserType)
purchases = spark.read.csv(purchases_path, header=True, inferSchema=True) # (PurchaseID,UserID,ProductID,PurchaseDate)

# NOTE, for a SQL-native alternative use:
#   - .filter("StartTime LIKE '201%' OR StartTime LIKE '2020%'")
#   - .filter(YEAR(STR_TO_DATE(StartTime)) <= 2020 AND ...)
def year(purchaseDate):
    return int(purchaseDate[:4])
spark.udf().register("year", year)

italian_users = (
    users
    .filter("Country == 'Italian'")
    .select("UserID", "Type")
) # (UserID, Type)

purchases_2010_to_2020 = (
    purchases
    .filter("year(purchaseDate) >= 2010 AND year(purchaseDatee) <= 2020")
    .select("PurchaseID", "UserID")
) # (PurchaseID, UserID)

(
    purchases_2010_to_2020
    .join(italian_users, on="UserID", how="inner")
    .groupBy("Type")
    .count()
    .withColumnRenamed("count(1)", "Purchases")
    .write.csv(output_path_1)
)

# ------------------------------------
# Part 2

"""
Number of purchases in 2023 for each Italian user who did not make purchases
from 2024. The second part of this application considers only the Italian users
without purchases from 2024. Considering only that subset of users, the second
part of this application computes the number of purchases in 2023 for each of those
users. The value for each user must be returned even when it is zero. The
result is stored in the second output folder. The output contains one line for each
Italian user without purchases from 2024. The output format is as follows:
UserID, number of purchases made by UserID in 2023
"""

purchases_from_2024 = (
    purchases
    .filter("year(purchaseDate) >= 2024")
    .select("PurchaseID", "UserID")
    )

inactive_italian_users_from_2024 = (
    italian_users.join(purchases_from_2024, on="UserID", how="left_anti")
)

purchases_in_2023 = (
    purchases
    .filter("year(purchaseDate) = 2023")
    .select("PurchaseID", "UserID")
)

(
    purchases_in_2023
    .join(inactive_italian_users_from_2024, on="UserID", how="right")
    .groupBy("UserID")
    # NOTE: we cannot just .count(), nor agg({"*": "count"}) as it would include rows with PurchaseID = None / NaN / Na,
    # if we instead call .agg({"PurchaseID": "count"}) we force the system to only count items with non-null values in that specific column \
    # i.e., the below purchaseID will automatically ignore None items returned from the right outer join above
    # as a result, we dont even have to care about using .fillna(0) for such items
    # count(PurchaseID) will automatically count such users who did not buy anything in 2023 as 0
    .agg{"PurchaseID": "count"}
    .withColumnRenamed("count(PurchaseID)", "Purchases")
    .write.csv(output_path_2)
)



















