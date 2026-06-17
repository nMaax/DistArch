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

"""
Categories with a higher number of sales in 2023 than in 2022. The first part of this
application considers only the years 2022 and 2023. It selects the categories with a
total number of sales in 2023 greater than the total number of sales in 2022. Store
the selected categories in the first output folder (one category per output line).
"""

catalogue = spark.read.csv(catalogue_path, header=True, inferSchema=True) # (ItemID,Name,Category)
daily_sales = spark.read.csv(daily_sales_path, header=True, inferSchema=True) # (ItemID,Date,NumberOfSales)

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


# ---- OR ----

# start with the daily sales RDD and keep data about year 2022 and year 2023
# and then compute the following pair RDD
# key = itemID
# value = (#sales in 2022, #sales in 2023)
def filter_year(line):
    fields = line.split(',')
    date = fields[1]
    return date.startswith('2023') or date.startswith('2022')

def get_2022_2023_sales_count(line):
    fields = line.split(',')
    key = fields[0]
    date = fields[1]
    sales = int(fields[2])
    if date.startswith('2022'):
        sales2022, sales2023 = sales, 0
    else:
        sales2022, sales2023 = 0, sales
    return (key, (sales2022, sales2023))


sales_2022_2023 = sales_rdd \
                    .filter(filter_year) \
                    .map(get_2022_2023_sales_count)

# get the catalogue and compute for each item its own category
# key = itemID
# value = category
def get_category(line):
    fields = line.split(',')
    item_id = fields[0]
    category = fields[2]
    return item_id, category

categories = catalogue_rdd.map(get_category)

# join the two RDDs and get the following RDD
# key = itemID
# value = category, (#sales 2022, #sales 2023)
# and map it into
# key = category
# value = #sales 2022, #sales 2023
# and finally sum all the sales together with a reduceByKey
sales_per_category = categories \
                        .join(sales_2022_2023) \
                        .map(lambda x: (x[1][0], (x[1][1][0], x[1][1][1]))) \
                        .reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1]))

# filter and keep only the entries associated with
# #sales 2023 > #sales 2022
res1 = sales_per_category.filter(lambda x: x[1][1] > x[1][0])
res1.keys().saveAsTextFile('out1')

# ---- OR ----

# Define a UDF for extracting the year
def get_year(date):
    return int(date.split('/')[0])

# Register the UDF
spark.udf.register("Year", get_year, IntegerType())


# Define a UDF that returns +1 if the year is 2023. Otherwise, -1
def is_2023(date):
    if get_year(date)==2023:
        return +1
    else:
        return -1

# Register the UDF
spark.udf.register("IS_2023", is_2023, IntegerType())

res1_df = spark.sql(
    """SELECT Category """ +
    """FROM dailySales, catalogue """ +
    """WHERE dailySales.itemID=catalogue.itemID """ +
    """  AND (Year(Date)=2022 OR Year(Date)=2023) """ +
    """GROUP BY Category """ +
    """HAVING SUM(NumberofSales*IS_2023(Date))>0 """
)

res1_df.write.csv('out1SQL',header=False)

# ------------------------------------
# Part 2

"""
For each item, select the dates with an increasing income compared to the previous
date. The second part of this application considers all 40 years of data. It selects,
for each item, the dates on which the daily income associated with the item is
greater than the daily income of the previous date. The daily income of an item on a
specific date is given by the number of sales of that item on that date multiplied by
the item's price on that date. Store the result in the second output folder (one pair
(ItemID, date with an increasing income compared to the previous date) per output
line).

Suppose the function nextDate(date) is provided. Given a date in the format
‘YYYY/MM/DD’, nextDate(date) returns the next date (again in the format
‘YYYY/MM/DD’). For example, nextDate(‘2018/04/05’) returns the date ‘2018/04/06’.
"""

# Register function given by exercise command
spark.udf.register("nextDate", nextDate)

prices = spark.read.csv(prices_path, header=True, inferSchema=True) # (ItemID,StartingDate,EndingDate,Price)

# NOTE: As shown in the example, we suppose daily_sales also contains 0-sales rows

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
            # this one instead will NOT work
            # ```
            # on="today.ItemID = yesterday.ItemID AND today.Date = yesterday.NextDate",
            # ```
        ),
        how="inner")
    .filter("today.TotalIncome > yesterday.TotalIncome")
    .select("today.ItemID", "today.Date")
    .write.csv(output_path_2, header=True)
)

# ---- OR ----

# first, we need to compute the income
# income = #sales * price
# starting from the sales RDD, we compute the following rdd
# key = itemID
# value = #sales, date
def get_sales_with_date(line):
    fields = line.split(',')
    item_id = fields[0]
    sales = int(fields[2])
    date = fields[1]
    return (item_id, (sales, date))

sales_with_date = sales_rdd.map(get_sales_with_date)

# compute for each item all the prices and the respective date range
# key = itemID
# value = (price, startDate, endDate)
def get_price_and_range(line):
    fields = line.split(',')
    item_id = fields[0]
    price = float(fields[3])
    start, end = fields[1], fields[2]
    return item_id, (price, start, end)

price_per_item = prices_rdd \
                    .map(get_price_and_range)

# join sales_with_date with price_per_item
# key = item_id
# value = (#sales, sales_date), (price, startDate, endDate)
# and use a filter to keep only those entries with sales_date in range (start. end) from the price table
# and use a mapValues to compute the income associated
# key = item_id
# value = date, income
def filter_by_date(data):
    item_id = data[0]
    (sales, sales_date), (price, start, end) = data[1]
    return start <= sales_date <= end

def compute_income(data):
    (sales, sales_date), (price, start, end) = data
    return sales_date, sales * price

income_per_item = sales_with_date \
                    .join(price_per_item) \
                    .filter(filter_by_date) \
                    .mapValues(compute_income)

# to compute the number of entries with an increasing income compared to the previous date
# we use flatMap+groupByKey()+filter()


def windowElements(pair):
    itemId=pair[0]
    date=pair[1][0]
    income=pair[1][1]

    winElements = []
    # First element - Current item, current date - first element of the window associated with (item, current date)
    winElements.append( ((itemId, date), (date, income)) )

    # second element - Current item, current date+1 - second element of the window associated with (item, current date+1)
    winElements.append( ((itemId, nextDate(date)), (date, income)) )

    return winElements


def increasingIncome(window):
    # Discard the last window that is incomplete
    if len(list(window[1]))==2:
        element1=list(window[1])[0]
        element2=list(window[1])[1]

        date1=element1[0]
        income1=element1[1]

        date2=element2[0]
        income2=element2[1]

        # Check if the income is increasing (second of the two dates in the temporal order associated with
        # the highest income)
        if ( (date1>date2 and income1>income2) or (date2>date1 and income2>income1)):
            return True
        else:
            return False
    else:
        return False


res2 = income_per_item \
        .flatMap(windowElements)\
        .groupByKey()\
        .filter(increasingIncome)\
        .keys()
res2.saveAsTextFile('out2')

# ---- OR ----

income_df = spark.sql(
    """SELECT dailySales.itemId, dailySales.Date, NumberofSales*Price as income """ +
    """FROM dailySales, prices """ +
    """WHERE dailySales.itemID=prices.itemID """ +
    """  AND dailySales.Date>=prices.StartingDate """ +
    """  AND dailySales.Date<=prices.EndingDate """
)

# Register dataframe income_df as temporary views
income_df.createOrReplaceTempView('incomes')

res2_df = spark.sql(
    """SELECT incomes_today.itemId, incomes_today.Date """ +
    """FROM incomes as incomes_today, incomes as incomes_previousday """ +
    """WHERE incomes_today.itemId=incomes_previousday.itemId """ +
    """  AND incomes_today.Date=nextDate(incomes_previousday.Date) """ +
    """  AND incomes_today.income>incomes_previousday.income """
)

res2_df.write.csv('out2SQL',header=False)
