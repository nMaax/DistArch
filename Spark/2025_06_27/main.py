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

Note. Suppose there is at least one purchase of at least one Italian user for each
user type from 2010 to 2020, i.e., no user type is associated with zero
purchases made by Italian users from 2010 to 2020
"""


# --- RDDs ---

users = sc.textFile(users_path) # (UserID,Age,Gender,Country,UserType)
purchases = sc.textFile(purchases_path) # (PurchaseID,UserID,ProductID,PurchaseDate)

italian_users = (
    users
    .map(lambda line: line.split(",")) # UserID = 0, Age = 1, Gender = 2, Country = 3, UserType = 4
    .filter(lambda items: items[3] == "Italian") # Select only Country == Italy
    .map(lambda items: (items[0], items[4])) # Make a pair UserID, UserType
)

purchases_2010_to_2020 = (
    purchases
    .map(lambda line: line.split(","))  # PurchaseID = 0, UserID = 1, ProductID = 2, PurchaseDate = 3
    .filter(lambda items: 2010 <= int(items[3][:4]) <= 2020) # Select only year betwen 2010 and 2020
    .map(lambda items: (items[1], 1)) # Make a pair UserID, 1
)

(
    italian_users
    .join(purchases_2010_to_2020) # UserID, (UserType, 1)
    .map(lambda pair: (pair[1][0], pair[1][1])) # UserType, 1
    .reduceByKey(lambda a, b: a + b) # UserType, numPurchases (sum of 1s)
    .map(lambda pair: f"{pair[0]},{pair[1]}")
    .saveAsTextFile(output_path_1)
)


# --- DataFrames ---

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
    .filter("year(purchaseDate) >= 2010 AND year(purchaseDate) <= 2020")
    .select("PurchaseID", "UserID")
) # (PurchaseID, UserID)

(
    purchases_2010_to_2020
    .join(italian_users, on="UserID", how="inner")
    .groupBy("Type")
    .count()
    .withColumnRenamed("count(1)", "Purchases")
    .write.csv(output_path_1, header=True) # Type, Purchases as first line
)


# --- SparkSQL ---

users.createOrReplaceTempView("users_view")
purchases.createOrReplaceTempView("purchases_view")

part1_sql = """
    SELECT
        u.Type,
        COUNT(*) AS Purchases
    FROM purchases_view p
    INNER JOIN users_view u ON p.UserID = u.UserID
    WHERE u.Country = 'Italian'
      AND SUBSTRING(p.PurchaseDate, 1, 4) BETWEEN '2010' AND '2020'
    GROUP BY u.Type
"""
spark.sql(part1_sql).write.csv(output_path_1, header=True)


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


# --- RDDs ---

users_purchased_from_2024 = (
    purchases
    .map(lambda line: line.split(",")) # PurchaseID = 0, UserID = 1, ProductID = 2, PurchaseDate = 3
    .filter(lambda items: int(items[3][:4]) >= 2024) # Select only years 2024 and beyond
    .map(lambda items: (items[1], None)) # Make a pair (UserID, None)
)

italian_users_no_purchases_2024 = (
    italian_users
    .subtractByKey(users_purchased_from_2024) # UserID, UserType
)

purchases_2023 = (
    purchases
    .map(lambda line: line.split(",")) # PurchaseID = 0, UserID = 1, ProductID = 2, PurchaseDate = 3
    .filter(lambda items: int(items[3][:4]) == 2023) # Select only year 2023
    .map(lambda items: (items[1], 1)) # Make a pair UserID, 1
    # NOTE: Pre-reducing data before the join will make Spark trigger a shuffle, which will also include users we do not care about,
    # however, Spark is designed to utilize map-side combining before any data hits the network, thus giving a improvement in optimality
    .reduceByKey(lambda a, b: a + b) # Pre-sum the numPurchases per user
)

(
    italian_users_no_purchases_2024
    .leftOuterJoin(purchases_2023) # (UserId, (UserType, 1 OR None))
    .mapValues(lambda value: value[1] if value[1] is not None else 0)
    .map(lambda pair: f"{pair[0]}, {pair[1]}")
    .saveAsTextFile(output_path_2)
)


# --- DataFrames ---

purchases_from_2024 = (
    purchases
    .filter("year(purchaseDate) >= 2024")
    .select("PurchaseID", "UserID")
    )

# NOTE: left_anti returns all entries who did NOT appear on the right dataframe
# therefore, the resulting dataframe has the same schema as the left one
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
    .agg({"PurchaseID": "count")}
    .withColumnRenamed("count(PurchaseID)", "Purchases")
    .write.csv(output_path_2, header=True) # UserID, Purchases as first line
)


# --- SparkSQL ---

part2_sql = """
    SELECT
        u.UserID,
        COUNT(p2023.PurchaseID) AS Purchases
    FROM users_view u
    LEFT JOIN purchases_view p2023
      ON u.UserID = p2023.UserID
     AND SUBSTRING(p2023.PurchaseDate, 1, 4) = '2023'
    WHERE u.Country = 'Italian'
      AND NOT EXISTS (
          SELECT 1
          FROM purchases_view p2024
          WHERE p2024.UserID = u.UserID
            AND SUBSTRING(p2024.PurchaseDate, 1, 4) >= '2024'
      )
    GROUP BY u.UserID
"""
spark.sql(part2_sql).write.csv(output_path_2, header=True)
