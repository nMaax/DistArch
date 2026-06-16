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

# Remind, there is at least one leacture for each course, so we dont have to find
# quirky courses (anyway, they would be intractable: would their ratio be 0% or 100% ?)
lectures = spark.read.csv(lectures_path, header=True, inferSchema=True)

(
    lectures
    .selectExpr("CID", "CASE WHEN Duration > 120 THEN 1 ELSE 0 END AS isLong") # (CID, isLong)
    .groupBy("CID")
    .agg({"*": "count", "IsLong": "sum"})
    .selectExpr("CID", "`sum(isLong)` / `count(*)` AS longRatio") # Or count(1), whatever it is called
    .filter("longRatio >= 0.7")
    .select("CID")
    .write.csv(output_path_1, header=True)
)


# ------------------------------------
# Part 2

user_watched_lecture = spark.read.csv(user_watched_lecture_path, header=True, inferSchema=True)

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
        on=["SID", "CID"]
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
