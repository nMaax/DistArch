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

courses_rdd = sc.textFile(courses_path)
lectures_rdd = sc.textFile(recorded_lectures_path)
visualizations_rdd = sc.textFile(student_watched_lecture_path)


lectures_rdd = (
    lectures_rdd
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], items[1])) # LID, CID
)

visualizations_rdd = (
    visualizations_rdd
    .map(lambda line: line.split(","))
    .map(lambda items: (items[2], (items[0], int(items[1][:4]))))  # LID, (SID, year)
    .filter(lambda pair: pair[1][1] in (2023, 2024)) # Only visualizations in 2023, 2024
)

visualizations_rdd = (
    visualizations_rdd
    .rightOuterJoin(lectures_rdd)  # LID, ((SID, year) or None, CID)
    .map(lambda items: (items[1][1], (items[0], items[1][0])))  # CID, (LID, (SID, year) or None)
)

courses_rdd = (
    courses_rdd.map(lambda line: (line.split(",")[0], None) # (CID, None)
)

visualizations_rdd = (
    visualizations_rdd
    .rightOuterJoin(courses_rdd) # CID, ((LID, (SID, year) or None) or None, None)
)

def visualizations_2324(pair):
    # Items is: CID, ((LID, (SID, year) or None) or None, None)
    CID = pair[0]

    # Throw away outer-most None
    LID_SID_year = pair[1][0] # (LID, (SID, year) or None) or None

    if LID_SID_year is None:
        # The course had no registered lectures
        return CID, (0, 0)

    # LID_SID_year --> LID, (SID, year) or None

    # Thorw away LID
    SID_year = LID_SID_year[1] # (SID, year) or None

    if SID_year is None:
        # The course had registered lectures, but no one watched them
        return CID, (0, 0)

    # SID_year --> SID, year

    # Throw away SID
    year = SID_year[1] # year

    count_23 = 0
    if year == 2023:
        count_23 = 1

    count_24 = 0
    if year == 2024:
        count_24 = 1

    return CID, (count_23, count_24)


def sum_2324(counts_A, counts_B):
    count_23_A, count_24_A = counts_A
    count_23_B, count_24_B = counts_B
    return count_23_A + count_23_B, count_24_A + count_24_B


# Map NaNs to 0, visualizations to 1, and then we sum all togheter
visualizations_rdd = (
    visualizations_rdd
    .map(visualizations_2324) # CID, (count_23{0, 1}, count_24{0, 1})
    .reduceByKey(sum_2324)  # CID, (count_23, count_24)
)

def minmax_2324(counts_A, counts_B):
    count_23_A, count_24_A = counts_A
    count_23_B, count_24_B = counts_B
    return min(count_23_A, count_23_B), max(count_24_A, count_24_B)


min23, min24 = (
    visualizations_rdd
    .values()
    .reduce(minmax_2324)  # This should result in just one line, with the two items min and max
)

(
    visualizations_rdd
    .filter(lambda items: items[1][0] == min23 and items[1][1] == min24)
    .saveAsTextFile(output2_path)
)

# %% --- Dataframes ---

# NOTE: HIGHLY INEFFICIENT! AVOID!!

courses_df = ss.read.load(
    courses_path,
    format="csv",
    header=True,
    infer_schema=True,
    delimiter=",",
)

lectures_df = ss.read.load(
    recorded_lectures_path,
    format="csv",
    header=True,
    infer_schema=True,
    delimiter=",",
)

visualizations_df = ss.read.load(
    student_watched_lecture_path,
    format="csv",
    header=True,
    inferSchema=True,
    delimiter=",",
)

# Prepare dataframes
courses_df = courses_df.select("CID")
lectures_df = lectures_df.select("LID", "CID")

ss.udf.register("parseYear", lambda time: int(time[:4]), IntegerType())
visualizations_df = visualizations_df.selectExpr(
    "SID", "parseYear(time) AS year", "LID"
)

# Process 2023, this will result in: SID, year, LID, CID
# with some SID entries to nan (the given LID had no visualization in 2023)
# and some other SID, LID entries to nan too (the given CID has no recorded lectures)
visualizations_2023_df = (
    visualizations_df.filter("year==2023")
    .drop("year")
    .join(lectures_df, on="LID", how="right_outer")
    .join(courses_df, on="CID", how="right_outer")
)


# We consider as visualizations only valid SIDs, if there is a NaN then it was an unseed lecture/course
def visualization(SID):
    if isinstance(SID, int):
        return 1
    else:  # SID is NaN or None
        return 0


ss.udf.register("visualization", visualization, IntegerType())
visualizations_2023_df = (
    visualizations_2023_df.selectExpr("visualization(SID) AS visualization", "CID")
    .groupBy("CID")
    .agg({"visualization": "sum"})
    .withColumnRenamed("sum(visualization)", "visualizations_2023")
)  # Result will be CID, visualization (in 2023)


# Process 2024, this will result in: SID, year, LID, CID
# with some SID entries to nan (the given LID had no visualization in 2023)
# and some other SID, LID entries to nan too (the given CID has no recorded lectures)
visualizations_2024_df = (
    visualizations_df.filter("year==2024")
    .drop("year")
    .join(lectures_df, on="LID", how="right_outer")
    .join(courses_df, on="CID", how="right_outer")
)
visualizations_2024_df = (
    visualizations_2024_df.selectExpr("visualization(SID) AS visualization", "CID")
    .groupBy("CID")
    .agg({"visualization": "sum"})
    .withColumnRenamed("sum(visualization)", "visualizations_2024")
)  # Result will be CID, visualization (in 2024)

# We join 2023 and 2024 visualization counts
# There could be courses who didnt have visualizations either in 2023 or 2024, in such case we fill them with 0
visualizations_2023_2024_df = visualizations_2023_df.join(
    visualizations_2024_df, on="CID", how="outer"
).fillna(0)

# Select max and min in 2023 and 2024
max24_min23 = visualizations_2023_2024_df.agg(
    {"visualizations_2024": "max", "visualizations_2023": "min"}
).first()
max24, min23 = max24_min23[0], max24_min23[1]

# Find it back in the dataframe, and write it on the file
visualizations_2023_2024_df.filter(
    f"visualizations_2023={min23} AND visualizations_2024={max24}"
).write.csv(output2_path)
