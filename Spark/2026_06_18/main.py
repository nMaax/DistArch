
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

...

# --- DataFrames ---

...

# --- SparkSQL ---

...


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

...

# --- DataFrames ---

...

# --- SparkSQL ---

...
