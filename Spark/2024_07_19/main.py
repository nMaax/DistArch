import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

students_path = 'students.txt'
courses_path = 'courses.txt'
lectures_path = 'lectures.txt'
user_watched_lecture_path = 'user_watched_lecture.txt'

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"

# ------------------------------------
# Part 1

"""
Courses with a high percentage of long lectures. The first part of this application
selects the courses with a percentage of long lectures greater than 70%. A lecture
is classified as a long lecture if it lasts at least 120 minutes. Store the identifiers
(CIDs) of the selected courses in the first output folder (one selected CID per output
line).

Note: Suppose there is at least one lecture for each course
"""

# --- RDDs ---

lectures = sc.textFile(lectures_path) # (NUML,CID,Title,Date,StartingHour,Duration,Recorded)

(
    lectures
    .map(lambda line: line.split(","))
    .map(lambda items: (items[1], (1 if float(items[5]) >= 120 else 0, 1))) # CID, ({0, 1} (Counter for long lencture), 1 (Counter for lecture))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) # CID, (NumLongLecture, NumLectures)
    .filter(lambda pair: pair[1][0] / pair[1][1] > 0.7) # NumLongLecture/NumLectures > 0.7
    .keys()
    .saveAsTextFile(output_path_1)
)

# --- DataFrames ---


lectures = spark.read.csv(lectures_path, header=True, inferSchema=True) # (NUML,CID,Title,Date,StartingHour,Duration,Recorded)

(
    lectures
    .selectExpr("CID", "CASE WHEN Duration >= 120 THEN 1 ELSE 0 END AS isLong") # (CID, isLong)
    .groupBy("CID")
    .agg({"*": "count", "IsLong": "sum"})
    .selectExpr("CID", "`sum(isLong)` / `count(*)` AS longRatio") # Or count(1), whatever it is called
    .filter("longRatio > 0.7")
    .select("CID")
    .write.csv(output_path_1, header=True)
)

# --- Spark SQL ---

# Register the DataFrame as an accessible SQL table
lectures.createOrReplaceTempView("lectures_table")

part1_sql = """
    SELECT CID
    FROM lectures_table
    GROUP BY CID
    HAVING SUM(CASE WHEN Duration >= 120 THEN 1 ELSE 0 END) / COUNT(*) > 0.7
"""

spark.sql(part1_sql).write.csv(output_path_1, header=True)

# ------------------------------------
# Part 2

"""
For each student, the courses for which the student never watched more than one
time the course's recorded lectures. The second part of this application selects, for
each student, the courses for which he/she has never watched each of the course's
recorded lectures more than one time (i.e., for each student, select the courses for
which the student watched from 0 to 1 time each recorded lecture). For each
student, consider only the courses for which the student watched at least one
recorded lecture. Store the result in the second output folder (one of the selected
combinations (SID, CID) per output line).
"""

# --- RDDs ---

user_watched_lecture = sc.textFile(user_watched_lecture_path) # (SID,StartWatchingTime,NUML,CID)

def all_unique_lectures(pair):
    SID, CID = pair[0]
    lectures = pair[1]

    lectures_list = list(lectures)
    lectures_set = set(lectures)

    if len(lectures_list) == len(lectures_set):
        return True
    else:
        return False

(
    user_watched_lecture
    .map(lambda line: line.split(","))
    .map(lambda items: ((items[0], items[3]), items[2])) # (SID, CID), NUML
    .groupByKey()
    .filter(all_unique_lectures)
    .keys()
    .saveAsTextFile(output_path_2)
)

# However, this means that the groupBy will include a large amount of numbers which will be stored in main memory,
# thus, highly active students will end up clogging the worknode; we can rather do

invalid_SID_CID = (
    user_watched_lecture
    .map(lambda line: line.split(","))
    .map(lambda items: ((items[0], items[3], items[2]), 1)) # (SID, CID, NUML), 1
    .reduceByKey(lambda a, b: a+b)
    .filter(lambda pair: pair[1] > 1)
    .map(lambda pair: (pair[0][0], pair[0][1])) # SID, CID
)

(
    user_watched_lecture
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], items[3])) # SID, CID
    # NOTE: We could call distinct either before (left and/or right sidewise) or after the subtraction, leading to 4 scenarios:
    # Only After: [A, A, B, B, B, C, C, D, E, F, F] \ [A, B, B, D, F, F] = [C, C, E] on which we would call distinct and get [C, E]
    # Before, left only: [A, B C, D, E, F] \ [A, B, B, D, F, F] = [C, E]
    # Before, right only: [A, A, B, B, B, C, C, D, E, F, F] \ [A, B, D, F] = [C, C, E] then we would need to call it again
    # Before, both right and left: [A, B, C, D, E, F] \ [A, B, D, F] = [C, E]
    # Among all options better to do distinct BEFORE the subtract for saving network resources for the subtraction
    .distinct()
    .subtract(invalid_SID_CID)
    .saveAsTextFile(output_path_2)
)

# --- DataFrames ---

user_watched_lecture = spark.read.csv(user_watched_lecture_path, header=True, inferSchema=True) # (SID,StartWatchingTime,NUML,CID)

# By design, all lectures in user_watched_lecture are exclusively the recorded ones
# as well as ignoring all such lectures never watched by any students
times_watched_recorded_lecture_per_student = (
    user_watched_lecture
    .select("SID", "CID", "NUML") # Some of these will be duplicate
    .groupBy("SID", "CID", "NUML") # With the aggregation we make rows unique
    .agg({"*": "count"}) # Counting the duplicates, i.e., times the same student watched the same lesson
    .withColumnRenamed("count(*)", "timesWatched") # (SID, CID, NUML, timesWatched)
)

too_many_times_watched_course_lecture = (
    times_watched_recorded_lecture_per_student
    .filter("timesWatched > 1")
    .select("SID", "CID")
) # If a SID, CID pair appears here, then the student watched at least one lesson of such course too many times (>1)
# NOTE: we don't have to call distinct as it would be wasted resources, we work knowing we have duplicate (SID, CID)

(
    times_watched_recorded_lecture_per_student
    .join(
        too_many_times_watched_course_lecture,
        on=["SID", "CID"],
        how="anti"
        # Remove entries in times_watched_recorded_lecture_per_student that appear in too_many_times_watched_course_lecture
    )
    .select("SID", "CID")
    .distinct()
    .write.csv(output_path_2, header=True)
)

# NOTE: alternatively, use `subtract`, but remind:
#   -   Unlike RDDs, the DataFrame .subtract() method mimics SQL's EXCEPT operator.
#       Because of this, it strictly requires both DataFrames to have the exact
#       same schema (same number of columns, same data types, and same column order).
#   -   In the DataFrame API, .subtract() automatically eliminates duplicate rows from
#       the result (again, just like SQL EXCEPT).
#       If you used the select().subtract() approach shown above,
#       you wouldn't even need the final .distinct() call at the end
#       of your chain. The output of subtract() is already guaranteed to be distinct.
#   -   We dont need to care about calling .distinct() on left or right of the anti join as Spark Catalyst Optimizer handles it for us
#       (we can simply call it later)

# --- SparkSQL ---

# Register the raw watch log DataFrame as an accessible SQL table
user_watched_lecture.createOrReplaceTempView("watches_table")

part2_sql = """
    -- STEP 1: Get all unique pairs where a student watched at least one lecture
    SELECT DISTINCT SID, CID
    FROM watches_table

    EXCEPT

    -- STEP 2: Remove pairs where ANY single lecture was watched more than once
    SELECT SID, CID
    FROM (
        SELECT SID, CID, NUML, COUNT(*) as watch_count
        FROM watches_table
        GROUP BY SID, CID, NUML
    )
    WHERE watch_count > 1
"""

# Execute and write out
spark.sql(part2_sql).write.csv(output_path_2, header=True)
