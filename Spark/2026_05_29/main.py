

################################################################
### THIS IS THE SIMULATION DONE IN CLASS, NOT AN ACTUAL EXAM ###
################################################################


import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_path = 'users.txt'
meetings_path = 'meetings.txt'
invitations_path = 'invitations.txt'
participations_path = 'participations.txt'

users_rdd = sc.textFile(users_path)
meetings_rdd = sc.textFile(meetings_path)
invitations_rdd = sc.textFile(invitations_path)
participations_rdd = sc.textFile(participations_path)


# ------------------------------------
# Part 1

"""
Users who frequently organize meetings with too many expected participants. The first part of this application
selects the users who frequently organize meetings with too many expected participants. Specifically,
a user is selected if more than 15 of the meetings the user organized are characterized by a number of potential
participants greater than the maximum number of allowed participants. A user is considered a potential participant
in a meeting if he/she answers ‘Yes’ or ‘Unknown’ to the invitation to that meeting (i.e., Accept=’Yes’ or Accept=’Unknown’).
Store the identifiers (UIDs) of the selected users in the first HDFS output folder. Specifically, store one UID per output line.
"""


# --- RDDs ---

meetings = (
    meetings_rdd # (MID,Title,StartTime,EndTime,OrganizerUID,MaxParticipants)
    .map(lambda line: line.split(","))
    .map(lambda items: items[0], (item[4], int(item[5])) ) # MID, (OrganizerUID, MaxParticipants)
)

potential_participants = (
    invitations_rdd # (MID,UID,Accepted)
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], 1 if items[2] in {"Yes", "Unknown"} else 0)) # MID, PotentialParticipant{0,1}
    .reduceByKey(lambda a, b: a + b) # MID, TotPotentialParticipants
)

meetings_w_potential_participants = (
    meetings
    .join(potential_participants) # MID, ((OrganizerUID, MaxParticipants), TotPotentialParticipants)
    .mapValues(lambda values: (values[0][0], values[1], values[0][1])) # MID, (OrganizerUID, TotPotentialParticipants, MaxParticipants)
).cache() # NOTE: Used in Part 2

(
    meetings_w_potential_participants
    .filter(lambda items: items[1][1] > items[1][2]) # TotPotentialParticipants > MaxParticipants
    .map(lambda items: (items[1][0], 1)) # Organizer_UID, 1
    .reduceByKey(lambda a, b: a+b) # Organizer_UID, NumSaturatedMeetings
    .filter(lambda items: items[1] > 15) # NumSaturatedMeetings > 15
    .map(lambda items: items[0]) # Organizer_UID
    .saveAsTextFile(output_path)
)


# --- DataFrames ---

meetings = spark.read.csv(meetings_path, inferSchema=True, header=True) # (MID,Title,StartTime,EndTime,OrganizerUID,MaxParticipants)
invitations = spark.read.csv(invitations_path, inferSchema=True, header=True) # (MID,UID,Accepted)

potential_participants_df = (
    invitations
    .filter("Accepted = 'Yes' OR Accepted = 'Unknown'")
    .groupBy("MID")
    .count()
    .withColumnRenamed("count(1)", "potential_participants") # (MID, potential_participants)
)

meetings_w_potential_participants_df = (
    meetings
    .join(potential_participants_df, on="MID", how="inner") # (MID, organizer_UID, max_participants, potential_participants)
).cache() # NOTE: Used in Part 2

(
    meetings_w_potential_participants_df
    .filter("potential_participants > max_participants")
    .groupBy("organizer_UID")
    .count()
    .withColumnRenamed("count(1)", "exceeding_meetings") # (organizer_UID, exceeding_meetings)
    .filter("exceeding_meetings > 15")
    .select("organizer_UID")
    .write.csv(output_path)
)

# --- SparkSQL ---

meetings.createOrReplaceTempView("meetings")
invitations.createOrReplaceTempView("invitations")

query_part1 = """
WITH potential_counts AS (
    -- Count potential participants for each meeting
    SELECT
        MID,
        COUNT(*) AS pot_participants
    FROM invitations
    WHERE Accepted IN ('Yes', 'Unknown')
    GROUP BY MID
)
-- Join with meetings, filter by saturation, and count per organizer
SELECT
    m.OrganizerUID
FROM meetings m
JOIN potential_counts p ON m.MID = p.MID
WHERE p.pot_participants > m.MaxParticipants
GROUP BY m.OrganizerUID
HAVING COUNT(m.MID) > 15
"""

(
    spark.sql(query_part1)
    .write.csv(output_path_1)
)

# ------------------------------------
# Part 2

"""
The number of meetings with many potential participants and a few actual participants organized by each user. The second part
of this application considers only the users who organized at least one meeting and computes for each of them the
number of organized meetings characterized by less than 2 unique actual participants (i.e., no users or at
most one user actually participated in the meeting) and more than 10 potential participants (the definition of potential
participant is the same reported in the first part). Store the result in the second HDFS output folder. Specifically,
there is one output line for each user who organized at least one meeting and the number of meetings organized by that user
that satisfy the conditions reported in this second part of the problem specification. Those users who organized zero
meetings that satisfy the conditions of interest are not part of the result.
"""


actual_participants = (
    participations_rdd
    .map(lambda line: line.split(","))
    .map(lambda items: (items[0], items[1])) # (MID, UID)
    .distinct() # Remove repeating UID, MID lines for users who joined the same meeting multiple times
    .mapValues(lambda v: 1) # (MID, 1); for each user who participated in MID
    .reduceByKey(lambda a, b: a + b) # (MID, num_of_actual_participants)
)

(
    meetings_w_potential_participants # MID, (OrganizerUID, TotPotentialParticipants, MaxParticipants)
    .filter(lambda items: items[1][1] > 10) # potential_participants > 10
    .leftOuterJoin(actual_participants) # (MID, ((organizer_UID, potential_participants, max_participants), num_of_actual_participants or None))
    .mapValues(
        lambda values: (values[0][0], values[0][1], values[0][2], 0 if values[1] is None else values[1])
    ) # (MID, (organizer_UID, potential_participants, max_participants, actual_participants))
    .filter(lambda items: items[1][3] < 2) # actual_participants < 2
    .map(lambda items: (items[1][0], 1)) # (Organizer_UID, 1)
    .reduceByKey(lambda a, b: a + b)\ # Organizer_UID, num_meetings_meeting_the_constraint
    .saveAsTextFile(output_path)
)


# --- DataFrames ---

participations = spark.read.csv(participations_path, inferSchema=True, header=True) # (MID,UID,JoinTimestamp,LeaveTimestamp)

actual_participants_df = (
    participations_df
    .select("MID", "UID") # (MID,UID)
    .distinct() # Remove users who joined the same meeting multiple times
    .groupBy("MID")
    .count()
    .withColumnRenamed("count(1)", "actual_participants")  # (MID, actual_participants)
)

(
    meetings_w_potential_participants_df # (MID, organizer_UID, max_participants, potential_participants)
    .filter("potential_participants > 10")
    .join(actual_participants_df, on="MID", how="left")
    .fillna(0)  # (MID, organizer_UID, max_participants, potential_participants, actual_participants)
    .filter("actual_participants < 2")
    .groupBy("organizer_UID")
    .count() # (organizer_UID, num_meetings_meeting_the_constraint)
    .write.csv(output_path)
)


# --- SparkSQL ---

participations.createOrReplaceTempView("participations")

query_part2 = """
WITH potential_counts AS (
    -- Reusing the potential participants logic (filtering for > 10 early)
    SELECT
        MID,
        COUNT(*) AS pot_participants
    FROM invitations
    WHERE Accepted IN ('Yes', 'Unknown')
    GROUP BY MID
    HAVING COUNT(*) > 10
),
actual_counts AS (
    -- Count unique actual participants per meeting
    SELECT
        MID,
        COUNT(DISTINCT UID) AS act_participants
    FROM participations
    GROUP BY MID
)
-- Join everything and apply the final constraint
SELECT
    m.OrganizerUID,
    COUNT(m.MID) AS exceeding_meetings
FROM meetings m
JOIN potential_counts p ON m.MID = p.MID
LEFT JOIN actual_counts a ON m.MID = a.MID
WHERE COALESCE(a.act_participants, 0) < 2
GROUP BY m.OrganizerUID
"""

(
    spark.sql(query_part2)
    .write.csv(output_path_2)
)















