import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

ss = SparkSession.builder.getOrCreate()
sc = SparkContext

students_path = "students.txt"
courses_path = "courses.txt"
recorded_lectures_path = "recorded_lectures.txt"
student_watched_lecture_path = "student_watched_lecture.txt"

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"


# ------------------------------------
# Part 1

"""
Number of distinct lectures of the course CID10 watched by each student in each
year from 2015 to 2020. The first part of this application considers only the
visualizations from 2015 to 2020. The first part of this application computes the
number of distinct lectures of the course with CID “CID10” watched by each student
in each of the years from 2015 to 2020. Store the result in the first output folder. The
output contains one line for each combination (student, year), where year ranges
from 2015 to 2020. The output format is as follows: SID,year,number of distinct
lectures of the course identified by CID10 watched by this student (SID) in this specific year

Note. Do not consider the combinations (student, year) for which the student watched no lectures
of the course CID10 in year, i.e., the combinations (student, year) with no visualizations must not be considered.
"""


# --- RDDs ---

recorded_lectures = sc.textFile(recorded_lectures_path) # (LID,CID,Title,Duration)
student_watched_lecture = sc.textFile(student_watched_lecture_path) # (SID,StartWatchingTime,LID)

recorded_CID10_lectures = (
    recorded_lectures
    .map(lambda line: line.split(","))
    .filter(lambda items: items[1] == "CID10") # CID = "CID10"
    .map(lambda items: (items[0], None)) # LID, None
)

student_watched_lecture_15_20 = (
    student_watched_lecture
    .map(lambda line: line.split(","))
    .filter(lambda pair: 2015 <= int(items[1][:4]) <= 2020) # 2015 <= year <= 2020
    .map(lambda items: (items[2], (items[0], int(items[1][:4])))) # LID, (SID, year)
    .distinct() # A student could have seen the same lesson multiple times during the same year (same SID, same year, same LID)
)

# NOTE:
# Inner join will discard:
#   - all recorded lessons of CID10 that have not been seen by any student, in any year
#   - all (student, year) pairs that did not see any lesson belonging to CID10
#
# OPTIMIZATION: WHY WE CALL DISTINCT() BEFORE JOIN()?
#
# REASON 1: CARTESIAN EXPLOSION PREVENTION (Hypothetical risk)
# If 'recorded_CID10_lectures' was poorly formatted and contained duplicate LIDs, joining BEFORE
# distinct() would cause a catastrophic Cartesian cross-product. Multiple duplicate keys on both
# sides multiply together (N x M), which exponentially bloats memory and could cause a OOM crashe.
#
# REASON 2: NETWORK PAYLOAD REDUCTION (Our main reason)
# Even though the CID10 table has unique keys (no explosion risk), joining first would force
# Spark to ship every single duplicate student-watch record across the network, execute the join
# on all of them, and then shuffle them a second time to dedupe them at the end. By calling
# distinct() early, Spark destroys duplicates locally on the worker nodes, drastically shrinking
# the data payload before the join shuffle even begins.
# Note that BOTH distinct() and join() trigger heavy network shuffles. However, distinct() performs a
# map-side combine (deduplicating locally on worker nodes FIRST) before sending the remaining
# data across the network for global deduplication.
(
    student_watched_lecture_15_20
    .join(recorded_CID10_lectures) # (LID, ((SID, year), None))
    .map(lambda items: ((items[1][0][0], items[1][0][1]), 1)) # (SID, year), 1
    .reduceByKey(lambda v1, v2: v1 + v2) # (SID, year), TotWatchedLessonsOfCID10
    .map(lambda pair: f"{pair[0][0]},{pair[0][1]},{pair[1]}")
    .saveAsTextFile(output1_path)
)


# %% --- DataFrames ---

recorded_lectures = ss.read.load(recorded_lectures_path, format="csv", header=True, inferSchema=True, delimiter=",",) # LID, CID, title, duration
student_watched_lecture = ss.read.load(student_watched_lecture_path, format="csv", header=True, inferSchema=True, delimiter=",",) # SID, time, LID

recorded_lectures = (
    recorded_lectures
    .filter("CID=='CID10'")
    .select("LID")
)

# NOTE, for a SQL-native alternative use:
#   - .filter("StartTime LIKE '201%' OR StartTime LIKE '2020%'")
#   - .filter(YEAR(STR_TO_DATE(StartTime)) <= 2020 AND ...)
ss.udf.register("parseYear", lambda time: int(time[:4]), IntegerType())

student_watched_lecture = (
    student_watched_lecture
    .selectExpr("SID", "parseYear(time) AS year", "LID")
    .filter("year >= 2015 AND year <= 2020")
    .distinct()
)

(
    student_watched_lecture
    .join(recorded_lectures, on="LID", how="inner")
    .groupBy("SID", "year")
    .count()
    .withColumnRenamed("count(*)", "TotWatchedLessonsOfCID10")
    .write.csv(output1_path, header=False)
)
