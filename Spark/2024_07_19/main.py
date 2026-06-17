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
"""

# Remind, there is at least one leacture for each course, so we dont have to find
# quirky courses (anyway, they would be intractable: would their ratio be 0% or 100% ?)

lectures = spark.read.csv(lectures_path, header=True, inferSchema=True) # (NUML,CID,Title,Date,StartingHour,Duration,Recorded)

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

# ---- OR ----

def get_cid_longflag(line: str):
    # key = CID
    # value = (+1/0, +1)
    # +1 if it's a long lecture, 0 otherwise
    fields = line.split(',')
    cid = fields[1]
    duration = int(fields[-2])
    return (cid, (1 if duration > 120 else 0, 1))

# get for each lecture the CID and a flag: +1 if long, 0 otherwise,
# and use a reduceByKey to sum, for each CID, its total duration
# and finally use a mapValues to compute the percentage of long lectures
# key = CID
# value = percentage of long lectures
# To conclude, the filter on 70% threshold is applied
long_lecture_percentage = lectures_rdd \
                    .map(get_cid_longflag) \
                    .reduceByKey(lambda i1, i2: (i1[0] + i2[0], i1[1] + i2[1])) \
                    .filter(lambda x: x[1][0] / x[1][1] > 0.7)

# obtain the CIDs associated to courses with at least 70% of long lectures
# and save them
res1 = long_lecture_percentage.keys()
res1.saveAsTextFile('output1')

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

# ---- OR ----

# Retrieve for each student the watched recorded lectures.
# All lectures in UsersWatchedRecordedLectures.txt are recorded lectures by definition.
# A join with lectures is not needed.

# Count the number of times each recorded lecture was watched by each student
# key = SID, NumL, CID
# value = number of times the lecture was watched by student SID
def get_sid_cid_numl(s: str):
    fields = s.split(',')
    sid = fields[0]
    numl = fields[2]
    cid = fields[3]

    return ((sid, numl, cid), 1)

sid_lecture_count = watched_lectures_rdd.map(get_sid_cid_numl) \
                    .reduceByKey(lambda x1, x2: x1 + x2).cache()


# For each students, select the lectures watched more than one time.
# Those are used to identify the combinations (sid, cid) to discard
# Determine the invalid combinations with a filter and keep sid and cid only
invalid_cid_sid = sid_lecture_count.filter(lambda x: x[1] > 1)\
                                    .map(lambda p: (p[0][0], p[0][2]))


# Select the distinct combinations (sid, cid) for which sid watched at least one recorded lecture of cid
sid_watched_cid = sid_lecture_count.map(lambda p: (p[0][0], p[0][2]))\
                                    .distinct()

# Remove from sid_watched_cid the invalid combinations (sid, cid)
# and extract the keys
res2 = sid_watched_cid\
            .subtract(invalid_cid_sid)
res2.saveAsTextFile('output2')
