import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

catalogue_path = 'catalogue.txt'
prices_path = 'prices.txt'
daily_sales_path = 'daily_sales.txt'

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"

# ------------------------------------
# Part 1

catalogue = spark.read.csv(catalogue_path, header=True, inferSchema=True)
daily_sales = spark.read.csv(daily_sales_path, header=True, inferSchema=True)

# Avoid udf to save some resources
daily_sales_22_23 = (
    daily_sales
    .selectExpr("*", "SUBSTRING(Date, 1, 4) AS Year") # Assuming 1-based indexing, including last index (4)
    .filter(F.col("Year").isin("2022", "2023")) # We use strings because we used substrings before (alternative: use YEAR + STR_TO_DATE to make it an integer)
)

daily_sales_22_23_w_cat = (
    daily_sales_22_23
    .join(catalogue, on="ItemID", how="inner")
)

# NOTE (How to use pivot)
#
# Conceptually, a pivot rotates your data from a vertical layout (rows) into a horizontal layout (columns).
#
# To do this, Spark needs three pieces of information, which is why groupBy, pivot, and agg form an inseparable trio.
# Think of it as defining a grid:
#
# 1)    .groupBy("Category") (The Rows)
#       This tells Spark, "Every unique item in this column gets its own row."
#
# 2)    .pivot("Year") (The Columns)
#       This tells Spark, "Look at the 'Year' column. Every unique
#       value you find there (e.g., 2022, 2023) will become a brand-new, standalone column header."
#
# 3)    .agg(F.sum("NumberOfSales")) (The Cells)
#       This tells Spark, "At the intersection of each Category and Year, calculate this value."
#
# DOES PIVOT SHUFFLES?
#
# If you look at pivot() strictly by itself, it is just a metadata configuration step.
# In fact, if you try to run just daily_sales.groupBy("Category").pivot("Year") without an .agg(),
# Spark won't even give you a DataFrame back—it returns a RelationalGroupedDataset object.
# It’s just a blueprint waiting for instructions.

tot_sales_22_23_per_cat = (
    daily_sales_22_23_w_cat
    #.select(F.col("Category"), F.col("Year"), F.col("NumberOfSales")) # This is not truy needed
    .groupBy(F.col("Category"))
    .pivot("Year", values=["2022", "2023"]) # Explicitly listing values speeds up pivot performance
    .agg(F.sum("NumberOfSales"))
    .withColumnRenamed("2022", "TotSale_22")
    .withColumnRenamed("2023", "TotSale_23")
    .na.fill(0, ["TotSale_22", "TotSale_23"]) # Handles cases where a category had sales in only one of the years
)

# NOTE: When you provide the explicit values ["2022", "2023"] to pivot,
# Spark's Catalyst Optimizer completely rewrites your pivot query into a
# standard groupBy with conditional aggregations.
#
# Behind the scenes, your code effectively becomes this:
#
# ```
# (
#     daily_sales_22_23_w_cat
#     .groupBy("Category")
#     .agg(
#         F.sum(F.when(F.col("Year") == "2022", F.col("NumberOfSales"))).alias("2022"),
#         F.sum(F.when(F.col("Year") == "2023", F.col("NumberOfSales"))).alias("2023")
#     )
# )
# ```

# NOTE: Without using pivot you should have done
#
# ```
# tot_sales_22_23_per_cat = (
#     daily_sales_22_23_w_cat
#     #.select(F.col("Category"), F.col("Year"), F.col("NumberOfSales")) # This is not truy needed
#     .groupBy(F.col("Category"), F.col("Year"))
#     .agg({"NumberOfSales": "sum"})
#     .withColumnRenamed("sum(NumberOfSales)", "TotSales")
# )
#
# tot_sales_22_per_cat = tot_sales_22_23_per_cat.filter(F.col("Year") == 2022).withColumnRenamed("TotSales", "TotSales_22")
# tot_sales_23_per_cat = tot_sales_22_23_per_cat.filter(F.col("Year") == 2023).withColumnRenamed("TotSales", "TotSales_23")
#
# pivoted_tot_sales_22_23_per_cat = (
#     tot_sales_22_per_cat
#     .join(tot_sales_23_per_cat, on="Category", how="inner")
# )
# (
#    pivoted_tot_sales_22_23_per_cat
#     .filter(F.col("TotSale_22") < F.col("TotSale_23"))
#     .select(F.col("Category"))
#     .write.csv(output_path_1)
# )
# ```
# However this is less optimal

(
    tot_sales_22_23_per_cat
    .filter(F.col("TotSale_22") < F.col("TotSale_23"))
    .select(F.col("Category"))
    .write.csv(output_path_1)
    #.option("header", "true") # for including the head "Category"
)


# ------------------------------------
# Part 2

# Register function given by exercise command
spark.udf.register("nextDate", nextDate)

# As shown in the example, we suppose daily_sales also contains 0-sales rows

prices = spark.read.csv(prices_path, header=True, inferSchema=True)

daily_income = (
    daily_sales
    .join(prices, on="ItemID", how="inner")
    .filter("Date >= StartingDate AND Date <= EndingDate")
    .selectExpr("ItemID", "Date", "nextDate(Date) as NextDate", "Price * NumberOfSales AS TotalIncome")
)

# NOTE:
#
# Naming the python variables in two ways would not work: in Python's memory,
# you have created two pointers (daily_income and daily_income_aux)
# pointing to the exact same DataFrame object. However, when those instructions
# are compiled and sent to the Spark JVM, Spark strips away your Python variable
# names entirely. To Spark, both of those variables represent the exact same logical
# node in its execution plan with the exact same column names: ItemID, Date, and TotalIncome.
#
# .alias() injects a distinct identifier directly into Spark's
# internal metadata. It gives the DataFrame a temporary "SQL namespace"
# that survives the trip from Python to the JVM

today = daily_income.alias("today")
yesterday = daily_income.alias("yesterday")

(
    today
    .join(
        yesterday,
        on=(
            # NOTE: this works from Spark 3.x+, otherwise it would not distinguish variables from JVM objects
            (today["ItemID"]==yesterday["ItemID"])
            &
            (today["Date"]==yesterday["NextDate"])
            # NOTE, equivalently:
            #
            # ```
            # on=(
            #   (F.col("today.ItemID") == F.col("yesterday.ItemID"))
            #   &
            #   (F.col("today.Date") == F.col("yesterday.NextDate"))
            # ),
            # ```
            #
            # Or even
            # ```
            # on="today.ItemID = yesterday.ItemID AND today.Date = yesterday.NextDate",
            # ```
        ),
        how="inner")
    .filter("today.TotalIncome > yesterday.TotalIncome")
    .select("today.ItemID", "today.Date")
    .write.csv(output_path_2, header=True)
)
