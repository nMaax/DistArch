# %% Imports

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# %% Session and init

conf = SparkConf().setAppName("Lab2").setMaster("local[*]")
sc = SparkContext(conf=conf)
ss = SparkSession.builder.getOrCreate()

# %% Input files

courses_path = "..."  # From input file
recorded_lectures_path = "..."  # From input file
student_watched_lecture_path = "..."  # From input file

# %% Output files

output2_path = "..."  # Whatever you want

# %% --- RDDs ---

# NOTE: the below is missing header removal!

courses_rdd = sc.textFile(courses_path)
lectures_rdd = sc.textFile(recorded_lectures_path)
visualizations_rdd = sc.textFile(student_watched_lecture_path)

courses_rdd = courses_rdd.map(
    lambda line: (line.split(",")[0], None)
)  # (CID, None), a value is required by Spark!
lectures_rdd = lectures_rdd.map(
    lambda line: (line.split(",")[0], line.split(",")[1])
)  # (LID, CID)
visualizations_rdd = visualizations_rdd.map(
    lambda line: (line.split(",")[2], (line.split(",")[0], int(line.split(",")[1][:4])))
)  # (LID, (SID, year))

# Filter onli visualizations in 2023, 2024
visualizations_rdd = visualizations_rdd.filter(
    lambda items: items[1][1] in (2023, 2024)
)

visualizations_rdd = visualizations_rdd.rightOuterJoin(
    lectures_rdd
)  # (LID, ((SID, year), CID))

visualizations_rdd = visualizations_rdd.map(
    lambda items: (items[1][1], (items[0], items[1][0]))
)  # (CID, (LID, (SID, year)))

# This will result in (CID, ((LID, (SID, year)), None))
# we can drop the None, then, due to the outer joins, we will have some rows
# where SID, year, and enventually LID are nan/None/null, those count as no visualizations for the given course
visualizations_rdd = visualizations_rdd.rightOuterJoin(courses_rdd)


def visualizations_2324(items):
    # Items is: (CID, ((LID, (SID, year)), None))
    CID = items[0]
    LID_SID_year_dummy = items[1]  # ((LID, (SID, year)), None)
    LID_SID_year, _ = LID_SID_year_dummy  # Throw away dummy

    if LID_SID_year is None:
        # The course had no registered lectures
        return CID, (0, 0)

    _, SID_year = LID_SID_year  # Thorw away LID

    if SID_year is None:
        # The course had registered lectures, but no one watch them
        return CID, (0, 0)

    _, year = SID_year  # Throw away SID

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
visualizations_rdd = visualizations_rdd.map(visualizations_2324).reduceByKey(
    sum_2324
)  # CID, (count_23, count_24)


def minmax_2324(counts_A, counts_B):
    count_23_A, count_24_A = counts_A
    count_23_B, count_24_B = counts_B
    return min(count_23_A, count_23_B), max(count_24_A, count_24_B)


min23, min24 = visualizations_rdd.values().reduce(
    minmax_2324
)  # This should result in just one line, with the two items min and max

visualizations_rdd.filter(
    lambda items: items[1][0] == min23 and items[1][1] == min24
).saveAsTextFile(output2_path)

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
