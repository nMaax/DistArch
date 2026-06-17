import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

customers_path = 'customers.txt'
tv_series_path = 'tv_series.txt'
episodes_path = 'episodes.txt'
customer_watched_path = 'customer_watched.txt'

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"

# ------------------------------------
# Part 1

"""
Total number of episodes for each comedy television series with a lifespan of at least
3650 days. The first part of this Spark application considers only the comedy television
series (genre equal to comedy) with a lifespan of at least 3650 days. The lifespan of a
television series is defined as the difference between the first and the latest on-air
dates associated with its episodes. For each comedy TV series with a lifespan of at
least 3650 days, compute the total number of episodes. Store the result in the first
HDFS output folder. The output contains one line for each comedy TV series with a
lifespan of at least 3650 days. Each output line is formatted as follows:
SID,Total number of episodes for this SID

Suppose there is a function called diffDatesInDays(firstDate,secondDate) that returns
the difference between secondDate and firstDate in terms of number of days. For
instance, the invocation diffDatesInDays(“2022/11/07”, “2022/11/13”) returns 6.
"""

spark.udf.register("diffDatesInDays", diffDatesInDays)

tv_series = spark.read.csv(tv_series_path, header=True, inferSchema=True) # (SID,Title,Genre)
episodes = spark.read.csv(episodes_path, header=True, inferSchema=True) # (SID,SeasonNumber,EpisodeNumber,Title, OriginalAirDate)

comedy_tv_series = (
    tv_series
    .filter("Genre = 'Comedy'")
)

commedy_episodes = (
    episodes
    .join(comedy_tv_series, on="SID", how="inner")
)

(
    commedy_episodes
    .selectExpr("SID", "STR_TO_DATE(OriginalAirDate, '%Y/%m/%d') AS OriginalAirDate") # SQL built-in function
    .selectExpr("SID", "OriginalAirDate AS OriginalAirDate1", "OriginalAirDate AS OriginalAirDate2") # Otherwise I cannot group them simultaneously on the agg()
    .groupBy("SID")
    .agg({"OriginalAirDate1": "max", "OriginalAirDate2": "min", "*": "count"})
    .withColumnRenamed("max(OriginalAirDate)", "latestAirDate")
    .withColumnRenamed("min(OriginalAirDate)", "firstAirDate")
    .withColumnRenamed("count(*)", "totEpisodes")
    .selectExpr("SID", "diffDatesInDays(firstAirDate, latestAirDate) AS lifespan", "totEpisodes")
    .filter("lifespan >= 3650")
    .select("SID", "totEpisodes")
    .write.csv(output_path_1, header=True)
)


# ------------------------------------
# Part 2

"""
The number of TV series completely watched by each customer. The second part of
the Spark application considers all television series and computes, for each customer,
the number of TV series for which the customer watched all episodes. Store the result
in the second HDFS output folder. Specifically, the second output folder must contain
one line for each customer with the following information:
CID,Number of TV series for which the customer CID watched all episodes

Note that all customers must be considered and stored in the second output folder
(also the customers with a number of TV series for which they watched all
episodes equal to zero)
"""

customers = spark.read.csv(customers_path, header=True, inferSchema=True) # (CID,Name,Surname,City,Country)
customer_watched = spark.read.csv(customer_watched_path, header=True, inferSchema=True) # (CID,StartTimestamp,SID,SeasonNumber,EpisodeNumber)

# Some of the rows in this dataframe will have null entries except for CID,
# those are users who never watched anything
customer_watched_augmented = (
    customer_watched
    .join(customers, on="CID", how="right")
    .select("CID", "StartTimestamp", "SID", "SeasonNumber", "EpisodeNumber")
) # (CID, StartTimestamp, SID, SeasonNumber, EpisodeNumber)

num_episodes_watched = (
    customer_watched_augmented
    .select("CID", "SID", "SeasonNumber", "EpisodeNumber")
    .distinct() # This will keep the null entries still null
    .groupBy(["CID", "SID"])
    # This will IGNORE the null entries as long as I dont use "*" (count = 0 if a user never watched anything)
    .agg({"EpisodeNumber":, "count"})
    .withColumnRenamed("count(EpisodeNumber)", "NumEpisodesWatched")
) # (CID, SID, NumEpisodesWatched)

num_episodes = (
    episodes
    .groupBy("SID")
    .count()
    .withColumnRenamed("count(1)", "NumEpisodes")
) # (SID, NumEpisodes)

(
    num_episodes_watched
    .join(num_episodes, on="SID", how="left") # Must preserve null SIDs
    .selectExpr("CID", "CASE WHEN NumEpisodesWatched=NumEpisodes THEN 1 ELSE 0 END AS Finished")
    .fillna(0) # The leftover of customers associated with null entries persists, so I fill their finished column with 0s
    .groupBy("CID")
    .agg({"Finished": "sum"})
    .withColumnRenamed("sum(Finished)", "TotSeriesFinished")
    .write.csv(output_path_2, header=True)
)
















