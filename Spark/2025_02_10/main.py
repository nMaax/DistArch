import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_path = 'users.txt'
meetings_path = 'meetings.txt'
invitations_path = 'invitations.txt'
participations_path = 'participations.txt'

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"

# ------------------------------------
# Part 1

"""
Users who frequently organized meetings with the maximum number of allowed
participants in 2024. The first part of this application selects the users who frequently
organized meetings in 2024 (meetings with StartTime associated with 2024) with a
number of actual participants equal to the maximum number of allowed participants
(MaxParticipants). Specifically, a user is selected if more than 20 of the meetings the
user organized in 2024 are characterized by a number of distinct actual participants
equal to the maximum number of allowed participants. A user is considered an actual
participant in a meeting if he/she participated in the meeting (according to the content
of Participations.txt). Store the identifiers (UIDs) of the selected users in the first HDFS
output folder. Specifically, store one UID per output line.

Note. Remind that the same user can participate multiple times in the same meeting.
"""

# --- RDDs ---

meetings = sc.textFile(meetings_path) # (MID,Title,StartTime,EndTime,OrganizerUID,MaxParticipants)
participations = sc.textFile(participations_path) # (MID,UID,JoinTimestamp,LeaveTimestamp)

meetings_2024 = (
    meetings
    .map(lambda line: line.split(",")) # MID=0,Title=1,StartTime=2,EndTime=3,OrganizerUID=4,MaxParticipants=5
    .filter(lambda items: int(items[2][:4]) == 2024)
    .map(lambda items: (items[0], (items[4], int(items[5])))) # MID, (OrganizerUID, MaxParticipants)
)

# NOTE: I could also filter participations to only those in 2024 assuming a participation to a meeting in 2024
# shall resonably happen in 2024 too. This way I can aleviate workload on distinct() as less rows will be processed
# I will then use JoinTimestamp as reference, ignoring edge cases for meetings done in new year eve for people who joined in 2025
# (to include such cases, I can simply check the joinTimestamp year is in {2024, 2025})
# However Spark is already smart enough during join to not share such rows which wont be involved in the inner-join
# thus we can ignore this!
unique_participations = (
    participations
    .map(lambda line: line.split(",")) # MID=0,UID=1,JoinTimestamp=2,LeaveTimestamp=3
    # .filter(lambda items: int(items[2][:4])==2024) # Reduce rows for faster shuffle later
    .map(lambda items: (items[0], items[1])) # MID, UID
    .distinct() # So that if a user entered and left the same meeting multiple times, it is not counted twice
)

num_unique_participations_per_meeting = (
    unique_participations
    .map(lambda pair: (pair[0], 1))
    .reduceByKey(lambda a, b:a+b) # MID, NumUniqueParticipants
)

(
    meetings_2024
    .join(num_unique_participations_per_meeting) # MID, ((OrganizerUID, MaxParticipants), NumUniqueParticipants)
    .mapValues(lambda value: (value[0][0], value[0][1], value[1])) # MID, (OrganizerUID, MaxParticipants, NumUniqueParticipants)
    .filter(lambda pair: pair[1][1] == pair[1][2]) # select MaxParticipants == NumUniqueParticipants
    .map(lambda pair: (pair[1][0], 1)) # OrganizerUID, 1
    .reduceByKey(lambda a, b: a + b) # OrganizerUID, NumFullMeetings
    .filter(lambda pair: pair[1] > 20)
    .map(lambda pair: pair[0])
    .saveAsTextFile(output_path_1)
)

# --- DataFrames ---

meetings = spark.read.csv(meetings_path, header=True, inferSchema=True) # (MID,Title,StartTime,EndTime,OrganizerUID,MaxParticipants)
participations = spark.read.csv(participations_path, header=True, inferSchema=True) # (MID,UID,JoinTimestamp,LeaveTimestamp)

# NOTE, for a SQL-native alternative use:
#   - .filter("StartTime LIKE '2024%'")
#   - .filter(YEAR(STR_TO_DATE(StartTime)) = 2024)
def year(timestamp):
    return int(timestamp[:4])
spark.udf().register("year", year) # Automatically infer return type

meetings_2024 = (
    meetings
    .filter("year(StartTime) = 2024")
    .select("MID", "OrganizerUID", "MaxParticipants")
) # (MID, OrganizerUID, MaxParticipants)

participations_2024 = (
    participations
    .select("MID", "UID")
    .distinct() # A user could have joined a meeting multiple times
    .join(meetings_2024, on="MID", how="inner")
) # (MID, UID, OrganizerUID, MaxParticipants)

# NOTE: OrganizerID and MaxParticipants have a functional dependency on MID;
# for every unique MID, there is exactly one organizer and one maximum participant value.
# Thus I can include them in the groupBy clause
actual_participations_2024 = (
    participations_2024
    .groupBy(["MID", "OrganizerID", "MaxParticipants"])
    .count()
    .withColumnRenamed("count(1)", "ActualParticipants")
) # (MID, OrganizerID, MaxParticipants, ActualParticipants)

full_meetings_2024 = (
    actual_participations_2024
    .filter("ActualParticipants == MaxParticipants")
    .groupBy("OrganizerUID")
    .count()
    .withColumnRenamed("count(1)", "numFullMeetings") #
) # (OrganizerID, numFullMettings)

(
    full_meetings_2024
    .filter("numFullMeetings > 20")
    .select("OrganizerID")
    .write.csv(output_path_1, header=True) # `OrganizerUID` as first line
)

# ------------------------------------
# Part 2

"""
Participation of the users in the meetings they organized in 2024. The year of interest is
again 2024. For each user who organized at least one meeting in the year 2024,
compute the number of meetings he/she organized but did not participate. Store the
result in the second HDFS output folder. Specifically, there is one output line for each
user who organized at least one meeting. Each line contains the UID of one of the
users who organized meetings, followed by the number of meetings UID organized in
2024 but did not participate in

Note. Those users who always participated in the meetings they organized must also
be stored in the second output folder (for those users, the number of meetings they
organized in 2024 but did not participate in is 0).
"""

# --- RDDs ---

(
    meetings_2024
    .leftOuterJoin(unique_participations) # MID, ((OrganizerUID, MaxParticipants), UID or None)
    # NOTE: None values will result in a false boolean above, thus giving isOrganizer=0
    .map(lambda pair: ((pair[0], pair[1][0]), int(pair[1][0][0] == pair[1][1]))) # (MID, OrganizerUID), isOrganizer{0,1} (if OrganizerUID==UID then 1, else 0)
    .reduceByKey(lambda a, b: a + b) # (MID, OrganizerUID), orgJoined{0,1} (1 if organizer joined)
    .map(lambda pair: (pair[0][1], 1-pair[1])) # OrganizerUID, orgNotJoined{0,1} (1 if organizer did not join)
    .reduceByKey(lambda a, b: a + b) # OrganizerUID, NumMeetingsOrganizedNotJoined
    .map(lambda pair: f"{pair[0]},{pair[1]}")
    .saveAsTextFile(output_path_2)
)

# --- DataFrames ---

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
    #
    #   NOTE: even if we sum 1s and 0s, logically we can never achieve values >1 in the two items as
    #   participations_2024 was built with a distinct() who removed duplicate MID, UID keys; while meetings_2024 has at most
    #   one entry with a given MID (primary key), and of course the associated OrganizerUID
    .filter("sum(is_org)=1") # Select only entries for organizers
    .selectExpr("UID", "IF(sum(is_part)==0, 1, 0) AS missed")
)

(
    meetings_with_organizer_presence.groupBy("UID")
    .agg({"missed": "sum"})
    .withColumnRenamed("UID", "OrganizerUID")
    .withColumnRenamed("sum(missed)", "totMissed")
    .write.csv(output_path_2, header=True) # OrganizerUID, totMissed as first line
)
