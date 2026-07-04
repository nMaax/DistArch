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


# --- RDDs ---

# NOTE: can we suppose all categories has been sold at least one time in 2022 or 2023? NO!

catalogue = sc.textFile(catalogue_path) # (ItemID, Name, Category)
daily_sales = sc.textFile(daily_sales_path) # (ItemID, Date, NumberOfSales)

items_to_categories = (
    catalogue
    .map(lambda line: line.split(",")
    .map(lambda items: (items[0], items[2])) # ItemID, Category
)

sales_22_23 = (
    daily_sales
    .map(lambda line: line.split(",")
    .filter(lambda items: int(items[1][:4]) in {2022, 2023})
    .map(lambda items: (items[0], (items[2], int(items[1][:4])))) # ItemID, (NumberOfSales, Year)
)

sales_22 = (
    sales_22_23
    .filter(lambda pair: pair[1][1] == 2022)
    .map(lambda pair: (pair[0], pair[1][0])) # ItemID, NumberOfSales
)

sales_23 = (
    sales_22_23
    .filter(lambda pair: pair[1][1] == 2023)
    .map(lambda pair: (pair[0], pair[1][0])) # ItemID, NumberOfSales
)

tot_sales_22_per_cat = (
    sales_22
    .rightOuterJoin(items_to_categories) # ItemID, (NumberOfSales or None, Category)
    .map(lambda pair: (pair[1][1], pair[1][0] if pair[1][0] is not None else 0)) # Category, NumberOfSales or None
    .reduceByKey(lambda a,b: a+b) # Category, TotNumberOfSales (in 2022)
)


tot_sales_23_per_cat = (
    sales_23
    .rightOuterJoin(items_to_categories) # ItemID, (NumberOfSales or None, Category)
    .map(lambda pair: (pair[1][1], pair[1][0] if pair[1][0] is not None else 0)) # Category, NumberOfSales or None
    .reduceByKey(lambda a,b: a+b) # Category, TotNumberOfSales (in 2023)
)

(
    tot_sales_22_per_cat
    .join(tot_sales_23_per_cat) # Category, (TotNumberOfSales_2022, TotNumberOfSales_2023)
    .filter(lambda pair: pair[1][1] > pair[1][0]) # TotNumberOfSales_2023 > TotNumberOfSales_2022
    .map(lambda pair: pair[0]) # Category
    .saveAsTextFile(output_path_1)
)

# COMPLEXITY? Tot Shuffles = 2 reduceByKey on small-table + 2 rightOuterJoin on small-table + 1 join on the same small tables
# While we do many joins in the above, it is also true that two of them (the rightOuterJoin for sales 22 and 23 on items_to_categories) are done on a halved table w.r.t a big one with all sales from 2022, 2023. Still, this creates some overhead in Spark which makes one only big-table faster.

# Using the one-big-beautiful join would mean...

def pivot_sales_per_year(pair): # ItemID, (NumberOfSales, Year)
    ItemID = pair[0]
    NumberOfSales = pair[1][0]
    Year = pair[1][1]

    if Year == 2022:
        return ItemId, (NumberOfSales, 0)
    if Year == 2023:
        return ItemId, (0, NumberOfSales)

pivoted_sales_22_23 = (
    sales_22_23
    .map(pivot_sales_per_year) # ItemID, (NumberOfSales_22, NumberOfSales_23)
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) # A_Sales22 + B_Sales_22, A_Sales_23 + B_Sales_23
) # ItemID, (TotNumberOfSales_22, TotNumberOfSales_23)

(
    pivoted_sales_22_23
    # NOTE: since (0, 0) items would be filtered anyway later via 0 > 0; we can directly skip this and use a simple inner join
    #.rightOuterJoin(items_to_categories) # ItemID, ((TotNumberOfSales_22, TotNumberOfSales_23) or Null, Category)
    #.map(lambda pair: (pair[1][1], ((0, 0) if pair[1][0] is None, else (pair[1][0][0], pair[1][0][1])))) # Category, (TotNumberOfSales_22, TotNumberOfSales_23)
    .join(items_to_categories) # ItemID, ((TotNumberOfSales_22, TotNumberOfSales_23), Category)
    .map(lambda pair:(pair[1][1], pair[1][0])) # Category, (TotNumberOfSales_22, TotNumberOfSales_23)
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) # A_Sales22 + B_Sales_22, A_Sales_23 + B_Sales_23
    .filter(lambda pair: pair[1][1] > pair[1][0]) # TotNumberOfSales_2023 > TotNumberOfSales_2022
    .map(lambda pair: pair[0]) # Category
    .saveAsTextFile(output_path_1)
)

# COMPLEXITY? Tot Shuffles = 1 reduceByKey on big-table + 1 rightOuterJoin on big-table + 1 reduceByKey again on the joined table (presumably tiny for categories being really few)
# On top of that, we reduced *before* doing the join, further optimizing our operations

# NOTE: maybe there exist an even more optimized implementation using some native pivot functionality for RDD?
# For example, handling directly a big Sales_22_23 table carrying (Category, Year) as a key and then pivotinon Year
# Answer: NO. RDDs do not have a pivot() function. You must make it manually like above


# --- DataFrames ---

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
# However this is less optimal.

(
    tot_sales_22_23_per_cat
    .filter(F.col("TotSale_22") < F.col("TotSale_23"))
    .select(F.col("Category"))
    .write.csv(output_path_1)
    #.option("header", "true") # for including the head "Category"
)


# --- SparkSQL ---

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


# --- RDDs ---

prices = sc.textFile(prices_path) # (ItemID,StartingDate,EndingDate,Price)

def expand_price_for_all_dates(pair):
    ItemID = pair[0]
    StartingDate, EndingDate, Price = pair[1]

    Price = int(Price)

    flatted_prices = []
    currDate = StartingDate
    while currDate != EndingDate:
        flatted_prices.append((ItemID, currDate), Price)
        currDate = nextDate(currDate)
    flatted_prices.append((ItemID, EndingDate), Price)

    return flatted_prices

flat_prices = (
    prices
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], (items[1], items[2], items[3]))) # ItemID, (StartingDate, EndingDate, Price)
    .flatMap(expand_price_for_all_dates) # (ItemID, Date), Price
)

daily_sales_pivoted_on_date = (
    daily_sales
    .map(lambda line: line.split(",")
    .map(lambda items: ((items[0], items[1]), int(items[2]))) # (ItemID, Date), NumberOfSales
)

# NOTE: while ReduceByKey and similar compute some pre-combination on the workload,
# joins in RDD interface do not apply such optimizations and will send ALL rows, even those which will not join with any other


# NOTE: while flat_prices will virtually include all possible dates over the 40 years
# daily_sales (or daily_sales_pivoted_on_date) could miss some rows for such Items who did not sell anything on a given Date
# thus we must use a rightOuterJoin to include all dates
daily_income = (
    daily_sales_pivoted_on_date
    .rightOuterJoin(flat_prices) # (ItemID, Date), (NumberOfSales or None, Price)
    .mapValues(lambda value: 0 if vale[0] is None else value[0] * value[1]) # (ItemID, Date), DailyIncome
)

# NOTE: this map will throw the very first date of all
# and create a "ghost" date that didn't exist in the original database (the day after the last date)
# this behaviour is fine as the very first date of all can never be in the result, while the day after
# the last will not join with any date in the daily_income RDD, naturally removing it (as long as we use a inner or a left-outer join)
daily_income_key_next_date = (
    daily_income_per_item
    .map(lambda pair: ((pair[0][0], nextDate(pair[0][1])), pair[1])) # (ItemID, nextDate), DailyIncome --- or, equivalently: (ItemID, Date), PrevDayDailyIncome
)

(
    daily_income
    .join(daily_income_key_next_date) # (ItemID, Date), (DailyIncome, PrevDayDailyIncome)
    .filter(lambda pair: pair[1][0] > pair[1][1]) # DailyIncome > PrevDayDailyIncome
    .map(lambda pair: f"{pair[0][0]}, {pair[0][1]}") # "ItemId,Date"
    .saveAsTextFile(output_path_2)
)

# COMPLEXITY: 1 join (quite big, ~<40years of data × ~40years of data) + 1 join (also quite big, ~40years of data × ~40years of data).
# We used flatMap to generate a row for every single day in the price range. Over 40 years, that is ~14,600 rows per item, regardless of whether the item sold or not. If the catalogue has 10,000 items, you generate 146 million rows before any joins happen. We can do better than this...

# Specifically, we notice that in the above implementation we are handling many days where there were no sales, i.e., sequences where daily incomes were 0 -> 0 -> 0 -> 0 -> 0 -> ...
# To avoid this, we can work directly with daily_income (which, remind, will NOT include those days where the item was not sold), and select only those days with 0 income that happened just before some other with non-0 income
# Say instead we wanted those days where income did not decrease, then the 0 -> 0 -> ... sequence would be relevant and the flatMap approach would be indeed the right solution

prices = (
    prices
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], (items[1], items[2], int(items[3])))) # ItemID, (StartingDate, EndingDate, Price)
)

daily_sales = (
    daily_sales
    .map(lambda line: line.split(",")
    .map(lambda items: (items[0], (items[1], int(items[2])))) # ItemID, (Date, NumberOfSales)
)

daily_incomes = (
    daily_sales
    .join(prices) # ItemID, ((Date, NumberOfSales), (StartingDate, EndingDate, Price))
    .filter(lambda pair: pair[1][1][0] <= pair[1][0][0] <= pair[1][1][1]) # StartingDate <= Date <= EndingDate
    .map(lambda pair: ((pair[0], pair[1][0][0]), pair[1][0][1] * pair[1][1][2])) # (ItemID, Date), DailyIncome
)

# NOTE: like before, this map will throw the very first date of all
# and create a "ghost" date that didn't exist in the original database
daily_incomes_shifted = (
    daily_incomes
    .map(lambda pair: ((pair[0][0], nextDate(pair[0][1])), pair[1])) # (ItemID, NextDate), DailyIncome === (ItemID, Date), PrevDayIncome
)

# e.g. if on monday there were 0 sales, on tusday some sales, and on wedensday some sales then
#   - daily_incomes         will NOT provide monday, but tusday and wedensday only (with their associated income)
#   - daily_incomes_shifted will NOT provide monday, NO tusday, but wedensday (associated with tusday income), and thursday (associated with wedensday income)
#
# Then:
#   - inner joining them, i will get NO monday, NO tusday, thursday associated to (tusday income, thursday income), and NO thursday
#   - left  joining them (left=daily_income), i will get NO monday, tusday with (tusday income, None), wedensday associated to (wedensday income, tusday income), NO thursday
#   - right joining them (left=daily_income), i will get NO monday, NO tusday, wedensday associated to (wedensday income, tusday income), thursday (associated with
#       thursday income, wedensday income)
#
# Thus I will use left join
(
    daily_incomes
    .leftOuterJoin(daily_incomes_shifted) # (ItemID, Date), (DailyIncome, PrevDayIncome or None)
    .mapValues(lambda value: (value[0], 0 if value[1] is None else value[1]))
    .filter(lambda pair: pair[1][0] > pair[1][1]) # DailyIncome > PrevDayIncome
    .map(lambda pair: f"{pair[0][0]}, {pair[0][1]}")
    .saveAsTextFile(output_path_2)
)

# Professor instead, provided another solution which uses the following simplifying assumption:
# Each item is associated with all the dates of the last 40 years,
# and each date of the last 40 years is associated with all items. Even if an
# item was not sold on a specific date, there is a line for that combination in
# DailySales.txt anyway, with NumberOfSales set to 0.
# This means that:
#   - 0 -> 0 -> ... sequences of sold items are already stored in daily_sales
#   - The flatMap approach thus becomes significantly worse, it still works but it is now much more heavy,
#       as there was no need to care about flat mapping all prices: one could have simply joined the tables
#       and be sure that none would miss some day of income being 0. When the dataset was sparse, the flatMap was
#       a clever (albeit heavy) engineering workaround to ensure you didn't miss data gaps.
#       But now that we know the dataset is dense, the flatMap is doing manual work that is entirely redundant.
#       COMPLEXITY:
#           Shuffle 1
#               Sales data sent: 14,600 rows.
#               Price data sent: Expand those 40 price intervals into 14,600 daily rows. So, you send 14,600 rows of prices
#               Total: 14,600 + 14,600 = 29,200 rows
#           Shuffle 2
#               You take the 14,600 daily incomes and create a shifted copy (14,600 rows)
#               You join them together on (ItemID, Date):
#               Total: 14,600 + 14,600 = 29,200 rows
#   - The second solution using the join on the partial tables still works, and becomes perfectly equivalent to the
#       professor solution below
#       COMPLEXITY:
#           Shuffle 1:
#               Sales rows sent over network: 14,600 rows
#               Price rows sent over network: 40 rows
#               Total: 14,600 + 40 = 14,640 rows
#           Shuffle 2:
#               Left side sent over network: 14,600 rows
#               Right side sent over network: 14,600 rows
#               Total: 14,600 + 14,600 = 29,200 rows

def get_sales_with_date(line):
    fields = line.split(',')
    item_id = fields[0]
    sales = int(fields[2])
    date = fields[1]
    return (item_id, (sales, date))

sales_with_date = sales.map(get_sales_with_date) # ItemID, (DailySales, Date)

def get_price_and_range(line):
    fields = line.split(',')
    item_id = fields[0]
    price = float(fields[3])
    start, end = fields[1], fields[2]
    return item_id, (price, start, end)

price_per_item = prices.map(get_price_and_range) # ItemID, (Price, StartingDate, EndingDate)

def filter_by_date(data):
    item_id = data[0]
    (sales, sales_date), (price, start, end) = data[1]
    return start <= sales_date <= end

def compute_income(data):
    (sales, sales_date), (price, start, end) = data
    return sales_date, sales * price

income_per_item = (
    sales_with_date
    .join(price_per_item) # ItemID, ((DailySales, Date), (Price, StartingDate, EndingDate))
    .filter(filter_by_date)  # StartingDate <= Date <= EndingDate
    .mapValues(compute_income) # ItemID, (Date, DailyIncome)
)

# NOTE: To compute the number of entries with an increasing income compared
# to the previous date, we use flatMap() -> groupByKey() -> filter()
def windowElements(pair): # Pair: ItemID, (Date, DailyIncome)
    itemId=pair[0]
    date=pair[1][0]
    income=pair[1][1]

    winElements = []

    # First element: (Item, Date), (Date, DailyIncome)
    winElements.append( ((itemId, date), (date, income)) )

    # Second element: (Item, NextDate), (Date, DailyIncome)
    winElements.append( ((itemId, nextDate(date)), (date, income)) )

    # NOTE: First element and Second element are not grouped togheter under one tuple, they are two distinct, subsequent, elements
    # of the list winElements: `winElements = [..., ((Item, Date), (Date, DailyIncome)), ((Item, NextDate), (Date, DailyIncome)), ... ]`
    return winElements


def increasingIncome(window):   # window: (Item, Date), [(Date, DailyIncome),  (Date, PrevDateIncome)]
                                # window[0]: (Item, Date)
                                # window[1]: [(Date, DailyIncome),  (PrevDate, PrevDateIncome)]

    # NOTE: groupByKey collect data as a `ResultIterable`, not as a list, so we need to convert it
    if len(list(window[1]))==2: # Discard the last window that is incomplete

        element1=list(window[1])[0]
        element2=list(window[1])[1]

        date1=element1[0]
        income1=element1[1]

        date2=element2[0]
        income2=element2[1]

        # Check if the income is increasing
        # (second of the two dates in the temporal order associated with the highest income)
        # NOTE: this must be done because groupByKey() does NOT guarantee the order of the elements inside the iterable
        if ((date1>date2 and income1>income2) or (date2>date1 and income2>income1)):
            return True
        else:
            return False
    else:
        return False


res = (
    income_per_item
    .flatMap(windowElements) # Row i  : (Item, Date), (Date, DailyIncome) --> Key is (Item, Date)
                             # Row i+1: (Item, NextDate), (Date, DailyIncome) === (Item, Date), (PrevDate, PrevDateIncome) --> Key is (Item, Date)
                             # Row i+2: ...
    .groupByKey()            # (Item, Date), [(Date, DailyIncome),  (Date, PrevDateIncome)] --> Key is (Item, Date), Value is a list of at most 2 (Date, Income) tuples
                             # NOTE: groupByKey collect data as a `ResultIterable`, not as a list as written in the example; and it does NOT guarantee order
                             # of the elements is preserved in the final iterable, so maybe we could have [(PrevDate, PrevDateIncome), (Date, DailyIncome)]
    .filter(increasingIncome)
    .keys()
)

res.saveAsTextFile('out2')

# COMPLEXITY:
#       Shuffle 1:
#           Sales rows sent over network: 14,600 rows
#           Price rows sent over network: 40 rows
#           Shuffle 1 Subtotal: 14,640 rows
#       Shuffle 2 (groupByKey):
#           Duplicated rows sent over network: 14,600 × 2 = 29,200 rows


# --- DataFrames (under professor assumption) ---

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


# --- SparkSQL (under professor assumption) ---

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
