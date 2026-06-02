import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_path = 'users.txt'
meetings_path = 'meetings.txt'
invitations_path = 'invitations.txt'
participations_path = 'participations.txt'

# ------------------------------------
# Part 1

# Dataframes we will need
meetings = spark.read.csv(meetings_path, header=True, inferSchema=True)
participations = spark.read.csv(participations_path, header=True, inferSchema=True)

# NOTE: probably there exist a SQL-native equivalent of this, in such case
# it would be better to adopt it in place of my udf. I just use UDF to be safe on the result
# since I can't recall the SQL equivalent right now
# e.g. .filter("StartTime LIKE '2024%'")
def year(timestamp):
    return int(timestamp[:4])
spark.udf().register("year", year) # automatically infer return type

# Filter and clean the data with only what is needed
meetings_2024 = meetings.filter("year(StartTime) = 2024").select("MID", "OrganizerUID", "MaxParticipants")

# Join on participations with inner join to automatically select only such participations we are interested on
# to disntinguish between a user who joins multiple times we must run a distinct() on the participations df
participations_2024 = participations.select("MID", "UID").distinct().join(meetings_2024, on="MID", how="inner")

# Aggregate UID for each meeting to get the number of participants
# since OrganzerUID and MaxParticipants are uniquely associated to MID, this groupBy wont crash
# as there is not ambiguity
actual_participations_2024 = (
    participations_2024
    .groupBy("MID", "OrganizerID", "MaxParticipants")
    .count()
    .withColumnRenamed("count(1)", "ActualParticipants") # this will result in (MID, OrganizerID, MaxParticipants, ActualParticipants)
)

# Count how many meetings with actual == max participants, per OrganizerID
full_meetings_2024 = (
    actual_participations_2024
    .filter("ActualParticipants == MaxParticipants")
    .groupBy("OrganizerUID")
    .count() # this will result in OrganizerID, numFullMettings
    .withColumnRenamed("count(1)", "numFullMeetings")
)

# Select IDs of the organizers that exceeded 20 of such meetings, and save the result
full_meetings_2024.filter("numFullMeetings > 20").select("OrganizerID").write.csv(output_dir1)

# ------------------------------------
# Part 2

# Make a list of meetings where the organizer joined its own meeting, we will subtract this from the whole meetings list
meetings_organizer_joined_itself = participations_2024.filter("UID = OrganizerUID").select("MID")

# All organizers
all_organizers = meetings_2024.select("OrganizerUID").distinct()

# left_anti makes us select all the meetings in meetins_2024 that do not appear in meetings_organizer_joined_itself, i.e., meetings where the organizer did NOT joined
# basically it acts like a subtract
meetings_organizer_not_joined_itself = (
    meetings_2024
    .join(meetings_organizer_joined_itself, on="MID", how="left_anti")
    .groupBy("OrganizerID")
    .count()
    .withColumnRenamed("count(1)", "NumMeetingsOrganizerDidNotJoinedItsOwnMeeting") # this results in Organizer, NumMeetingsOrganizerDidNotJoinedItsOwnMeeting
    .join(all_organizers, on="OrganizerUID", how="right")
    .fillna(0) # Those who where missing are those which did join every own organized meeting, thus they count to 0
)

# Save the result
meetings_organizer_not_joined_itself.write.csv(output_dir2)

# --- OR --- >>> MORE OPTIMAL <<<

# Organizer = 1, Participant = 0
org_df = meetings_2024.selectExpr(
    "MID",
    "OrganizerUID AS UID",
    "1 AS is_org",
    "0 AS is_part"
)

# Organizer = 0, Participant = 1
# We reuse participations_2024 from Part 1. Because it was created via an inner join
# with meetings_2024, it inherently ONLY contains valid 2024 data
part_df = participations_2024.selectExpr(
    "MID",
    "UID",
    "0 AS is_org",
    "1 AS is_part"
)

meetings_with_organizer_presence = (
    org_df
    .union(part_df) # Now this is the concatenation of the aboves
    .groupBy("MID", "UID")
    .agg({"is_org": "sum", "is_part": "sum"}) # aggregating is used to remove duplicates from the union
    # we basically builded a custom "join" with binary values, or more technically, a cogroup
    # which will produce unique MID, UID keys, associated to
    #   - (1, 0) if UID is the organizer of MID but did not join it
    #   - (0, 1) if UID is NOT the organizer of MID, and joined MID
    #   - (1, 1) if UID is the organizer of MID, and joined MID
    #   - (0, 0) logically means the user is nor the organizer, nor joined the meetins. This state is impossible
    #   - Note that even if we sum 1s and 0s, logically we can never achieve values >1 in the two items as
    #   participations_2024 was built with a distinct() who removed duplicate MID, UID keys; while meetings_2024 has at most
    #   one entry with a given MID (primary key), and of course the associated OrganizerUID
    .filter("sum(is_org)=1") # Select only entries for organizers
    .selectExpr("UID", "IF(sum(is_part)==0, 1, 0) AS missed") # clean up and rename the columns with something more readable
)


tot_missed_own_meetngs_per_organizer = (
    meetings_with_organizer_presence.groupBy("UID")
    .agg({"missed": "sum"})
    .withColumnRenamed("UID", "OrganizerUID")
    .withColumnRenamed("sum(missed)", "tot_missed")
)

tot_missed_own_meetngs_per_organizer.write.csv(output_dir2)
