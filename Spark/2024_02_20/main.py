import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_path = 'users.txt'
catalogue_path = 'catalogue.txt'
purchases_path = 'purchases.txt'

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"

# ------------------------------------
# Part 1

purchases = spark.read.csv(purchases_path, header=True, inferSchema=True)

# Better to use SQL built-in functions instead of broadcasting a udf
purchases_22_23_w_year = (
    purchases
    .filter("saleTimestamp LIKE '2022%' OR saleTimestamp LIKE '2023%'")
    .selectExpr("*", "SUBSTRING(saleTimestamp, 1, 4) AS saleYear")
    # NOTE:
    # For year, you can also use:
    #   `SELECT YEAR(STR_TO_DATE('2019/02/02-09:15:01', '%Y/%m/%d-%H:%i:%s')) AS saleYear;`
    # which would give you an integer!
) # (saleTimestamp, UserID, ItemID, salePrice, saleYear), saleTimestamp is NOT removed

user_purchases_count_22_23 = (
    purchases_22_23_w_year
    .groupBy("UserID", "saleYear")
    .agg({"*": "count"})
    .withColumnRenamed("count(*)", "numPurchases")
) # (UserID, saleYear, numPurchases)

max_user_purchases_count_22_23 = (
    user_purchases_count_22_23
    .select("saleYear", "numPurchases")
    .groupBy("saleYear")
    .max()
) # (saleYear, max(numPurchases))

# NOTE: UNFORTUNATELY THERE IS NO OTHER WAY THAN USING collect() TO ACCESS SUCH VALUES LOCALLY
#       THE ONLY BETTER ALTERNATIVE I CAN THINK OF IS TO DO A SELECT + FILTER/WHERE AND THEN CALL first()
#       THUS I WILL JUST USE JOIN IN SAKE OF A CLEAN CODE
#
# e.g.
#
# max_2022 = (max_user_purchases_count_22_23
#     .filter("saleYear = '2022'")
#     .first()["max(numPurchases)"]
# )
#
# max_2023 = (max_user_purchases_count_22_23
#     .filter("saleYear = '2023'")
#     .first()["max(numPurchases)"]
# )

(
    user_purchases_count_22_23
    .join(
        max_user_purchases_count_22_23,
        on=(
            (
                user_purchases_count_22_23["saleYear"] == max_user_purchases_count_22_23["saleYear"]
            ) & (
                user_purchases_count_22_23["numPurchases"] == max_user_purchases_count_22_23["max(numPurchases)"]
            )
            # NOTE: with functional it would be:
            #
            # from pyspark.sql.functions import col
            #
            #   ...
            #
            #   on=(col("saleYear") == col("saleYear") & col("numPurchases") == col("max(numPurchases)"))
            #
            # HOWEVER THIS WONT WORK AS saleYear IS AMBIGUOUS
        )
        how="right" # inner would work too
    ) # This will only keep those users matching with the max (right outer join)
    .select("UserID")
    .distinct() # If a user is top-purchases in both 2022 and 2023, they would appear both times
    .write.csv(output_path_1)
)

# ------------------------------------
# Part 2

catalogue = spark.read.csv(catalogue_path, header=True, inferSchema=True)

purchases_per_item_cat = (
    purchases_22_23_w_year
    .join(
        catalogue.select("ItemID", "Category"),
        on="ItemID",
        how="right" # So we can keep categories who did not appear on the purchases
    ) # saleTimestamp, UserID, ItemID, salePrice, saleYear, Category, Name, stillInProduction
    .select("UserID", "ItemID", "Category")
    .distinct()
    .groupBy("Category", "ItemID")
    .agg({"UserID": "count"}) # Counting "UserID" instead of "*" ensures NULLs are ignored, returning 0
    .withColumnRenamed("count(UserID)", "numUsers")
) # (Category, ItemID, numUsers)

max_purchases_per_cat = (
    purchases_per_item_cat
    .groupBy("Category")
    .agg({"numUsers": "max"})
) # (Category, max(numUsers))

(
    purchases_per_item_cat.filter("NOT numUsers=0")
    .join(
        max_purchases_per_cat,
        on=
        (
            (purchases_per_item_cat["Category"] == max_purchases_per_cat["Category"])
            &
            (purchases_per_item_cat["numUsers"] == max_purchases_per_cat["max(numUsers)"])
        ),
        how="right"
    ) # This will only keep those category, ItemID rows associated to largest numUsers
    .select(
        max_purchases_per_cat["Category"],  # Referenced max_purchases_per_cat["Category"] explicitly to eliminate ambiguity,
        "ItemID",
        "max(numUsers)" # I should NOT escape `max()` as this is a simple select
    )
    .selectExpr(
        "Category",
        # No items purchased means `Category, max(numUsers)` are `Category, 0`
        # Need to escape `max()` to avoid it being read as an operation
        "CASE WHEN `max(numUsers)`=0 THEN 'NoPurchases' ELSE ItemID END AS ItemID"
    )
    .write.csv(output_path_2)
)
















