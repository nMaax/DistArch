
import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

customers_path = 'customers.txt'
items_path = 'items.txt'
purchases_path = 'purchases.txt'

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"

customers_rdd = sc.textFile(customers_path) # (CID,YearOfBirth,Gender,Country)
items_rdd = sc.textFile(items_path) # (IID,Name,Category,SuggestedPrice)
purchases_rdd = sc.textFile(purchases_path) # (PID,CID,IID,PurchasesTimestamp)

# ------------------------------------
# Part 1

"""
Italian customer(s) with the maximum number of purchases in 2010. The first part of this application
considers only the purchases made in the year 2010 by Italian customers. It selects the identifier of the
Italian customer(s) associated with the maximum number of purchases in the year 2010 made by Italian
customers. Suppose that there is at least one purchase made by Italian customers in the year 2010. The
result is stored in the first output folder. In case of a tie, all customers associated with the maximum
value must be stored (each output line contains the identifier of one of the selected customers)
"""

# --- RDDs ---

purchases_2010 = (
    purchases_rdd # (PID,CID,IID,PurchasesTimestamp)
    .map(lambda line: line.split(","))
    .filter(lambda items: int(items[3][:4]) == 2010) # PurchasesTimestamp's Year = 2010
    .map(lambda items: (items[1], 1)) # (CID, 1)
    # NOTE: I could equivalently run this reduceByKey after the join on italian customers
    # however I prefer to do it here as it is less stressful for the network to reduce rows
    # rather than joining many togheter
    .reduceByKey(lambda a, b: a + b) # CID, TotPurchases (in 2010)
)


# NOTE: Alternatively, I could have collected non-italian customers
# and subtractByKey on purchases_2010; however it would be more expensive
# as there reasonably are more non-italians than italians in the database
italian_customers = (
    customers_rdd # (CID,YearOfBirth,Gender,Country)
    .map(lambda line: line.split(","))
    .filter(lambda items: items[3] == "IT") # Country = "IT" (or Italy, or whatever is the string)
    .map(lambda items: (items[0], None)) # CID, None
)

purchases_2010_by_italians = (
    purchases_2010
    # NOTE: this join removes all customers who purchased
    # something in 2010 but are not italian, and all italians who
    # didnt purchase anything in 2010, however, thanks to the
    # command "Suppose that there is at least one purchase made by Italian customers in the year 2010"
    # and since we only need to deal with max (no min values, or other quirky requests)
    # we don't have to deal with the case of a customer having 0 purchases in 2010 (it would be wiped by max
    # over some other italian customer who bought something in 2010, which is guaranteed to exist)
    .join(italian_customers) # CID, (TotPurchases, None)
    .mapValues(lambda value: value[0]) # CID, TotPurchases
)

max_purchases = (
    purchases_2010_by_italians
    .map(lambda pair: pair[1])
    # NOTE: reduce(lambda a, b: max(a, b)) is also a
    # good alternative (approx. same stress on networks)
    .max() # Returns an integer
)

(
    purchases_2010_by_italians
    .filter(lambda pair: pair[1] == max_purchases) # CID, TotPurchases
    .keys() # CID
    .saveAsTextFile(output_path_1)
)

# --- DataFrames ---

customers = spark.read.csv(customers_path, header=False, inferSchema=True).toDF("CID", "YearOfBirth", "Gender", "Country")
purchases = spark.read.csv(purchases_path, header=False, inferSchema=True).toDF("PID", "CID", "IID", "PurchasesTimestamp")

purchases_2010 = (
    purchases
    .filter("SUBSTRING(PurchasesTimestamp, 1, 4) == '2010'")
    .groupBy("CID")
    .count()
    .withColumnRenamed("count", "num_purchases")
)

italians = (
    customers
    .filter("Country == 'IT'")
    .select("CID")
)

italian_purchases_2010 = (
    purchases_2010
    .join(italians, on="CID", how="inner")
).cache()

max_purchases = italian_purchases_2010.agg({"num_purchases": "max"}).first()[0]

(
    italian_purchases_2010
    .filter(f"num_purchases == {max_purchases}")
    .select("CID")
    .write.csv(output_path_1)
)

# --- SparkSQL ---

customers.createOrReplaceTempView("customers")
purchases.createOrReplaceTempView("purchases")

query_part1 = """
WITH italian_purchases_2010 AS (
    -- Filter for 2010 and Italians, then count purchases per customer
    SELECT
        p.CID,
        COUNT(p.PID) AS num_purchases
    FROM purchases p
    JOIN customers c ON p.CID = c.CID
    WHERE SUBSTRING(p.PurchasesTimestamp, 1, 4) = '2010'
      AND c.Country = 'IT'
    GROUP BY p.CID
),
ranked_customers AS (
    -- Use a Window Function to rank them from highest to lowest
    SELECT
        CID,
        RANK() OVER (ORDER BY num_purchases DESC) as rank
    FROM italian_purchases_2010
)
-- Select only the customer(s) tied for 1st place
SELECT CID
FROM ranked_customers
WHERE rank = 1
"""

(
    spark.sql(query_part1)
    .write.csv(output_path_1)
)

# ------------------------------------
# Part 2

"""
Categories with fewer than 100 “sold in many years” items. The second part of this application focuses
on the items that are categorized as “sold in many years”. An item is considered an item "sold in many
years" if there are at least 10 years in each of which the item was purchased at least one time. The
second part of this application selects the categories for which the number of “sold in many years”
items is less than 100 (0 included). The result is stored in the second output folder (one selected
category per output line). The output format is as follows:
Category, Number of “sold in many years” items associated with this category

Note that the categories with 0 “sold in many years” items are part of the result.
"""

# --- RDDs ---

# NOTE: purchases_rdd only contains items which were purchased, unsold items are not included
items_sold_in_many_years = (
    purchases_rdd # (PID,CID,IID,PurchasesTimestamp)
    .map(lambda line: line.split(","))
    .map(lambda items: (items[2], int(items[3][:4]))) # IID, year
    .distinct() # IID, year
    .mapValues(lambda value: 1) # IID, 1
    .reduceByKey(lambda a, b: a + b) # IID, NumYears (number of distinct years the item was sold)
    # NOTE: equivalently, we could have mapped NumYears < 10 as 0 and Numyears >= 10 as 1
    # however, if we filter data we reduce the number of rows that will be used in the join
    # we can discard such rows because all items will anyway end up in the final result after the join
    # as the items_rdd provide them all (those who do not appear in items_sold_in_many_years simply are NOT
    # items sold in many years and their dummy counter will be 0)
    # basically we are moving the <10 -> 0, >=10 -> 1 mapping after the join to let it operate with less rows
    # (at least from items_sold_in_many_years' side)
    .filter(lambda pair: pair[1] >= 10) # NumYears >= 10
    .map(lambda pair: (pair[0], 1)) # IID, 1
)

# NOTE: items_rdd instead contains all items, including unsold ones
items_to_category = (
    items_rdd # (IID,Name,Category,SuggestedPrice)
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], items[2])) # IID, Category
)

(
    items_sold_in_many_years
    .rightOuterJoin(items_to_category) # # IID, (1 or None, Category)
    .map(lambda pair: (pair[1][1], 0 if pair[1][0] is None else pair[1][0])) # Category, 0 or 1
    .reduceByKey(lambda a, b: a + b) # Category, TotItemsSoldInManyYears (could be 0)
    .filter(lambda pair: pair[1] < 100) # TotItemsSoldInManyYears < 100
    .map(lambda pair: f"{pair[0]}, {pair[1]}") # "Category, TotItemsSoldInManyYears"
    .saveAsTextFile(output_path_2)
)

# --- DataFrames ---

items = spark.read.csv(items_path, header=False, inferSchema=True).toDF("IID", "Name", "Category", "SuggestedPrice")

frequent_items = (
    purchases
    .selectExpr("IID", "SUBSTRING(PurchasesTimestamp, 1, 4) AS year")
    .distinct() # Drops duplicate years for the same item
    .groupBy("IID")
    .count()
    .withColumnRenamed("count", "distinct_years")
    .filter("distinct_years >= 10")
    .select("IID")
)

freq_items_per_category = (
    frequent_items
    .join(items, on="IID", how="inner")
    .groupBy("Category")
    .count()
    .withColumnRenamed("count", "freq_items_count")
)

all_categories = (
    items
    .select("Category")
    .distinct()
)

(
    all_categories
    .join(freq_items_per_category, on="Category", how="left")
    .fillna(0, subset=["freq_items_count"])
    .filter("freq_items_count < 100")
    .write.csv(output_path_2)
)

# --- SparkSQL ---

items.createOrReplaceTempView("items")

query_part2 = """
WITH frequent_items AS (
    -- Find items bought in >= 10 distinct years
    SELECT IID
    FROM purchases
    GROUP BY IID
    HAVING COUNT(DISTINCT SUBSTRING(PurchasesTimestamp, 1, 4)) >= 10
),
freq_items_by_category AS (
    -- Join with the catalogue to find out which category these superstar items belong to
    SELECT
        i.Category,
        COUNT(f.IID) AS freq_items_count
    FROM frequent_items f
    JOIN items i ON f.IID = i.IID
    GROUP BY i.Category
),
all_categories AS (
    -- Create a master list of all categories to catch the ones with 0 superstar items
    SELECT DISTINCT Category
    FROM items
)
-- Left join, fill nulls with 0, and apply the final < 100 filter
SELECT
    a.Category,
    COALESCE(c.freq_items_count, 0) AS total_frequent_items
FROM all_categories a
LEFT JOIN freq_items_by_category c ON a.Category = c.Category
WHERE COALESCE(c.freq_items_count, 0) < 100
"""

(
    spark.sql(query_part2)
    .write.csv(output_path_2)
)
