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

"""
Users with the highest number of purchases in 2022 or 2023. Considering only the
purchases related to the years 2022 and 2023, the first part of this application aims
to find the users associated with the highest number of purchases in the years 2022
or 2023. Specifically, a user is selected if (i) the number of purchases of that user in
the year 2022 is equal to the maximum number of purchases in the year 2022
among all users or (ii) the number of purchases of that user in the year 2023 is
equal to the maximum number of purchases in the year 2023 among all users. The
first HDFS output folder must contain the identifiers of the selected users (one
UserId per output line).

Note: There is at least one purchase in the year 2022 and at least one purchase in
the year 2023 (i.e., you do not have to deal with a maximum number of purchases
equal to zero in this part of the problem).
"""


# --- RDDs ---

...


# --- DataFrames ---

purchases = spark.read.csv(purchases_path, header=True, inferSchema=True) # (SaleTimestamp,UserID,ItemID,SalePrice)

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


# --- SparkSQL ---

purchases2223DF.createOrReplaceTempView("purchases2223")
userCountPurchases2223DF.createOrReplaceTempView("userCountPurchases2223")
maxPurchases2223DF.createOrReplaceTempView("maxPurchases2223")

def year(timestamp):
    return int(timestamp.split("/")[0])

spark.udf.register("year", year, IntegerType())
purchases2223DF = spark.sql("""
    SELECT *
    FROM purchases
    WHERE Year(SaleTimestamp)=2022 OR Year(SaleTimestamp)=2023
""").cache()


userCountPurchases2223DF = spark.sql("""
    SELECT UserID,
    SUM(IF(year(SaleTimestamp)==2022, 1, 0)) AS Counter22,
    SUM(IF(year(SaleTimestamp)==2023, 1, 0)) AS Counter23
    FROM purchases2223
    GROUP BY UserID
""").cache()

maxPurchases2223DF = spark.sql("""
    SELECT MAX(Counter22) AS Max2022, MAX(Counter23) AS Max2023
    FROM userCountPurchases2223
""")

res1DF = spark.sql("""
    SELECT UserID
    FROM userCountPurchases2223, maxPurchases2223
    WHERE Counter22=Max2022 OR Counter23=Max2023
""")

res1DF.write.csv(outputPath1,header=False)


# ------------------------------------
# Part 2

"""
For each category, the items purchased by the largest amount of users in the last
two years (2022-2023). Considering only the purchases related to 2022 and 2023,
the second part of this application aims to find, for each category, the items
purchased by the maximum number of unique users over the two years inside each
category. If more than one item of the same category is associated with the
maximum number of unique users for that category, select all those associated with
the maximum value. Store the result in the second HDFS output folder (one pair
(category, selected item) per output line). Output format: Category,ItemId. Store the
pair (Category, “NoPurchases”) for the categories without purchases in the
period 2022-2023.
"""

# --- RDDs ---

...


# --- DataFrames ---

catalogue = spark.read.csv(catalogue_path, header=True, inferSchema=True) # (ItemID,Name,Category,StillinProduction)

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


# --- SparkSQL ---

itemCategoryDF.createOrReplaceTempView("itemCategory")
itemDistinctUsersPurchasesDF.createOrReplaceTempView("itemDistinctUsersPurchases")
itemCategoryPurchasesDF.createOrReplaceTempView("itemCategoryPurchases")
res2DF.createOrReplaceTempView("res2")

itemDistinctUsersPurchasesDF = spark.sql("""
    SELECT itemID, count(*) as NumDistinctUsers
    FROM (SELECT DISTINCT itemId, userID
    FROM purchases2223) AS DistintPurch
    GROUP BY itemID
""")

itemCategoryDF = spark.sql("""
    SELECT itemId, Category
    FROM catalogue
""").cache()

itemCategoryPurchasesDF = (
    itemCategoryDF
    .join(
        itemDistinctUsersPurchasesDF,
        itemCategoryDF.itemId==itemDistinctUsersPurchasesDF.itemID
    ).select(
        itemCategoryDF.itemId,
        itemCategoryDF.Category,
        itemDistinctUsersPurchasesDF.NumDistinctUsers
    ).cache()
)

res2DF = spark.sql("""
    SELECT itemCategoryPurchases.Category, itemId
    FROM itemCategoryPurchases,
        (SELECT Category, Max(NumDistinctUsers) as MaxPerCat
        FROM itemCategoryPurchases
        GROUP BY Category) AS MaximumsPerCategories
    WHERE itemCategoryPurchases.Category=MaximumsPerCategories.Category
    AND itemCategoryPurchases.NumDistinctUsers=MaximumsPerCategories.MaxPerCat
""")

# NOTE: The UNION (in the SQL language) removes duplicates.
# If there are many items associated to a Category without purchases
# only one record "Category, NoPurchases" is returned
res2FinalDF = spark.sql("""
    SELECT Category, "NoPurchases" AS itemId
    FROM itemCategory
    WHERE Category NOT IN (SELECT Category FROM res2)
    UNION SELECT * FROM res2
""")

res2FinalDF.write.csv(outputPath2,header=False)
