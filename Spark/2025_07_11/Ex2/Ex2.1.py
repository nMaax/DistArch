# %% Imports

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# %% Session and init

conf = SparkConf().setAppName("Lab2").setMaster("local[*]")
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

# %% Input files
students_path = "..."  # From input file
courses_path = "..."  # From input file
recorded_lectures_path = "..."  # From input file
student_watched_lecture_path = "..."  # From input file

# %% Output files
output1_path = "..."  # Whatever you want

# %% --- RDDs ---

# We will work on recorded_lectures_rdd and student_watched_lecture_rdd

# Load the RDDs
recorded_lectures_rdd = sc.textFile(recorded_lectures_path)
student_watched_lecture_rdd = sc.textFile(student_watched_lecture_path)

# NOTE: the below is missing header removal!

# 1. Filter recorded_lectures_rdd only on those belonging to CID10, then drop CID as it is useless now, as well as title and duration, remind to make a pair RDD


# Format: LID, CID, title, duration
recorded_CID10_lectures_rdd = recorded_lectures_rdd.filter(
    lambda line: line.split(",")[1] == "CID10"
).map(
    lambda line: (line.split(",")[0], line.split(",")[1])
)  # Make a pair with key = LID, value = CID


# 2. Filter student_watched_lecture_rdd only on those between 2015 and 2020, remind to parse year with a function, remind to make a pair RDD


# Custom function to parse year from the timestamp
def parseYear(line: str):
    SID, time, LID = line.split(",")
    year = int(time[:4])  # strings can be sliced, we expect format YYYY/MM/DD
    return LID, (SID, year)  # return a pair where key = LID and value = (SID, year)


# Format: (LID, (SID, year)), keep only years between 2015 and 2020
student_watched_lecture_2015_2020_rdd = (
    student_watched_lecture_rdd.map(lambda line: parseYear(line))
    .filter(lambda items: items[1][1] <= 2020)
    .filter(lambda items: items[1][1] >= 2015)
)

# 3. Join the two above, with an inner join, by key = LID (both RDDs must have such as their key)
# the inner join will discard:
#   - all lessons (LID) that have not been seen
#   - all (student, year) pairs that did not see any lesson belonging to CID10
# remind that by default in RDDs there is no "on" or "type", it always is a inner join on the key of pairRDDs


# Output Format: (LID, ((SID, year), CID))
students_watched_lecture_of_CID10_2015_2020_rdd = (
    student_watched_lecture_2015_2020_rdd.join(recorded_CID10_lectures_rdd)
)

# So we re-arrange it to be ((SID, year), LID), dropping CID
students_watched_lecture_of_CID10_2015_2020_rdd = (
    students_watched_lecture_of_CID10_2015_2020_rdd.map(
        lambda items: ((items[1][0][0], items[1][0][1]), items[0])
    )
)

# Note that:
#   - now (student, time) is not longer unique, as time has been replaced by year
#   - a student could have seen the same lesson multiple times in a year (same SID, same year, same LID)

# 4. Call distinct over LID, then count using map() + reduceByKey(), and save the result


students_watched_lecture_of_CID10_2015_2020_rdd.distinct().map(
    lambda items: ((items[0][0], items[0][1]), 1)
).reduceByKey(lambda v1, v2: v1 + v2).saveAsTextFile(output1_path)


# %% --- DataFrames ---

# Format: LID, CID, title, duration
recorded_lectures_df = ss.read.load(
    recorded_lectures_path,
    format="csv",  # Whatever it is
    header=True,  # Whatever it is
    inferSchema=True,  # Whatever it is
    delimiter=",",  # Whatever it is
)

# SID, time, LID
student_watched_lecture_df = ss.read.load(
    student_watched_lecture_path,
    format="csv",  # Whatever it is
    header=True,  # Whatever it is
    inferSchema=True,  # Whatever it is
    delimiter=",",  # Whatever it is
)

recorded_lectures_df = recorded_lectures_df.filter("CID=='CID10'").select("LID")

# Cant remind the specific API to define the datatype, I just wanted to indicate the return type is int
ss.udf.register("parseYear", lambda time: int(time[:4]), IntegerType())
student_watched_lecture_df = student_watched_lecture_df.selectExpr(
    "SID", "parseYear(time) AS year", "LID"
).filter("year >= 2015 AND year <= 2020")

student_watched_lecture_df.join(
    recorded_lectures_df,
    on="LID",
    how="inner",
).distinct().groupBy("SID", "year").count().write.csv(output1_path)
