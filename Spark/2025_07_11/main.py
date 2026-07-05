import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
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

from pyspark.sql.types import IntegerType

recorded_lectures = spark.read.load(recorded_lectures_path, format="csv", header=True, inferSchema=True, delimiter=",",) # LID, CID, title, duration
student_watched_lecture = spark.read.load(student_watched_lecture_path, format="csv", header=True, inferSchema=True, delimiter=",",) # SID, time, LID

recorded_lectures = (
    recorded_lectures
    .filter("CID=='CID10'")
    .select("LID")
)

# NOTE, for a SQL-native alternative use:
#   - .filter("StartTime LIKE '201%' OR StartTime LIKE '2020%'")
#   - .filter(YEAR(STR_TO_DATE(StartTime)) <= 2020 AND ...)
spark.udf.register("parseYear", lambda time: int(time[:4]), IntegerType())

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

# --- SparkSQL ---

recorded_lectures.createOrReplaceTempView("lectures")
student_watched_lecture.createOrReplaceTempView("views")

query = """
WITH cid10_lectures AS (
    SELECT LID
    FROM lectures
    WHERE CID = 'CID10'
),
parsed_views AS (
    SELECT
        SID,
        CAST(SUBSTRING(time, 1, 4) AS INT) AS year,
        LID
    FROM views
    WHERE CAST(SUBSTRING(time, 1, 4) AS INT) BETWEEN 2015 AND 2020
)
SELECT
    v.SID,
    v.year,
    COUNT(DISTINCT v.LID) AS TotWatchedLessonsOfCID10
FROM parsed_views v
JOIN cid10_lectures l ON v.LID = l.LID
GROUP BY
    v.SID,
    v.year
"""

(
    spark.sql(query)
    .write.csv(output1_path, header=False)
)


# ------------------------------------
# Part 2

"""
Course(s) with the maximum number of visualizations in 2024 and the minimum
number of visualizations in 2023. The second part of this application considers only
the visualizations related to the years 2023 and 2024. The second part of this
application calculates for each course the number of visualizations (each line of
StudentsWatchedRecordedLectures.txt is a visualization) in 2023 and the number
of visualizations in 2024. Then, it selects the course(s) associated with the
maximum number of visualizations in 2024 and the minimum number of
visualizations in 2023. In case of a tie, all courses associated with the maximum
value in 2024 and the minimum value in 2023 must be selected and stored in the
output folder. If there are no courses that satisfy both constraints, the output folder
is empty.

The result is stored in the second output folder (one selected course per output
line). The output format is as follows:
CID,Number of visualizations associated with this course in 2024,Number of
visualizations associated with this course in 2023

Note that the case with both values (the maximum number of visualizations in
2024 and the minimum number of visualizations in 2023) equal to zero must also
be considered.

Note that the output is empty if there are no courses that satisfy both
constraints.
"""


# %% --- RDDs ---

courses = sc.textFile(courses_path) # (CID,Title,ProductionYear)

visualizations_per_lesson_23_24 = (
    student_watched_lecture
    .map(lambda line: line.split(","))
    .filter(lambda items: int(items[1][:4]) in {2023, 2024})
    .map(lambda items: ((int(items[1][:4]), items[2]), 1)) # (Year, LID), 1
    # NOTE: I could potentially avoid this reduceByKey and the result would be the same
    # however I prefer to reduce the number of rows on which the join will operate,
    # being reduceByKey less stressful for network than a join with many rows
    .reduceByKey(lambda a, b: a+b) # (Year, LID), TotVisualizationLIDinYear
    .map(lambda pair: (pair[0][1], (pair[0][0], pair[1]))) # LID, (Year, TotVisualization)
)

# NOTE: maybe some recorded lessons were not viewed, then this contains ALSO such lectures
all_recorded_lessons = (
    recorded_lectures
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], items[1])) # LID, CID
)

def unroll_visualizations(pair):
    LID = pair[0]
    CID = pair[1][1]
    if pair[1][0] is None:
        return CID, (0, 0)
    else:
        Year = pair[1][0][0]
        TotVisualization = pair[1][0][1]

    if Year == 2023:
        return CID, (TotVisualization, 0)
    if Year == 2024:
        return CID, (0, TotVisualization)


# NOTE: maybe some courses were not recorded at all, then this contains ALSO such courses
all_courses = (
    courses
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], None)) # CID, None
)

def manage_no_digital_courses(pair):
    CID = pair[0]
    visualizations = pair[1][0]

    if visualizations is None:
        return CID, (0, 0)
    else:
        return CID, visualizations

visualizations_per_course_23_24 = (
    visualizations_per_lesson_23_24
    # NOTE: what if a recorded lesson did 0 views in 2023 and 2024?
    # We will then find such value in recorded_lectures but not in student_watched_lecture
    .rightOuterJoin(all_recorded_lessons) # LID, ((Year, TotVisualizations) or None, CID)
    .map(unroll_visualizations) # CID, (visualizations23, visualizations24); one line per lesson
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) # CID, (visualizations23, visualizations24)
    # NOTE: what if a course has no recorded lessons?
    # We will then find such value in courses but not in recorded_lessons
    .rightOuterJoin(all_courses) # CID, ((visualizations23, visualizations24) or None, None)
    .map(manage_no_digital_courses) # CID, (visualizations23, visualizations24)
)

def min_max_visualizations(c1, c2):
    c1_23, c1_24 = c1
    c2_23, c2_24 = c2

    min_23 = min(c1_23, c2_23)
    max_24 = max(c1_24, c2_24)

    return min_23, max_24

min_max = (
    visualizations_per_course_23_24
    .values() # Drop key, get: visualizations23, visualizations24
    .reduce(min_max_visualizations)
)

(
    visualizations_per_course_23_24
    .filter(lambda pair: pair[1] == min_max)
    .map(lambda pair: f"{pair[0]}, {pair[1][1]}, {pair[1][0]}")
    .saveAsTextFile(output_path_2)
)


# %% --- Dataframes ---

from pyspark.sql.functions import expr

courses = spark.read.load(courses_path, format="csv", header=True, inferSchema=True, delimiter=",",) # (CID,Title,ProductionYear)

views_parsed = (
    visualizations_df
    .selectExpr("LID", "CAST(substring(time, 1, 4) AS INT) AS year")
    .filter("year = 2023 OR year = 2024")
) # LID, year

views_agg = (
    views_parsed
    .groupBy("LID")
    .agg(
        expr("sum(case when year = 2023 then 1 else 0 end) AS views_23"),
        expr("sum(case when year = 2024 then 1 else 0 end) AS views_24")
    )
) # LID, views_23, views_24

course_agg = (
    views_agg
    .join(lectures_df, on="LID", how="right")
    .groupBy("CID")
    .agg(
        # NOTE: COALESCE is a fallback function. It looks at a list of values from left to right and returns the first non-null value it finds.
        expr("sum(coalesce(views_23, 0)) AS total_23"),
        expr("sum(coalesce(views_24, 0)) AS total_24")
    )
) # CID, total_23, total_24

final_df = (
    courses_df.select("CID")
    .join(course_agg, on="CID", how="left")
    .fillna(0, subset=["total_23", "total_24"])
) # CID, total_23, total_24

max24_min23 = final_df.agg(
    {"total_24": "max", "total_23": "min"}
).first()

max24, min23 = max24_min23[0], max24_min23[1]

(
    final_df
    .filter(f"total_23 = {min23} AND total_24 = {max24}")
    .write.csv(output2_path)
)


# --- SparkSQL ---

courses.createOrReplaceTempView("courses")
lectures.createOrReplaceTempView("lectures")
visualizations.createOrReplaceTempView("views")

query = """
WITH parsed_views AS (
    SELECT
        LID,
        CAST(SUBSTRING(time, 1, 4) AS INT) AS year
    FROM views
    WHERE CAST(SUBSTRING(time, 1, 4) AS INT) IN (2023, 2024)
),
agg_views AS (
    SELECT
        LID,
        SUM(CASE WHEN year = 2023 THEN 1 ELSE 0 END) AS views_23,
        SUM(CASE WHEN year = 2024 THEN 1 ELSE 0 END) AS views_24
    FROM parsed_views
    GROUP BY LID
),
course_views AS (
    -- Right join logic written cleanly as a LEFT JOIN from the lectures table
    SELECT
        l.CID,
        SUM(COALESCE(v.views_23, 0)) AS total_23,
        SUM(COALESCE(v.views_24, 0)) AS total_24
    FROM lectures l
    LEFT JOIN agg_views v ON l.LID = v.LID
    GROUP BY l.CID
),
final_counts AS (
    -- Final join to catch courses with zero recorded lectures
    SELECT
        c.CID,
        COALESCE(cv.total_23, 0) AS total_23,
        COALESCE(cv.total_24, 0) AS total_24
    FROM courses c
    LEFT JOIN course_views cv ON c.CID = cv.CID
)
SELECT * FROM final_counts
"""

final_df = spark.sql(query).cache()

min23_max24 = final_df.selectExpr("MIN(total_23)", "MAX(total_24)").first()

(
    final_df
    .filter(f"total_23 = {min23_max24[0]} AND total_24 = {min23_max24[1]}")
    .write.csv(output2_path)
)



