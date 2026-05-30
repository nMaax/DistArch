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

# prepare meetings RDD
def parseMeeting(line: str):
    MID, title, start_time, end_time, organizer_UID, max_participants = line.split(",")
    return MID, (organizer_UID, int(max_participants))

meetings_rdd = meetings_rdd.map(parseMeeting) # pairRDD with (MID, (organizer_UID, max_participants))

# prepare RDD of potential participants per-meeting
def parseInvitation(line: str):
    MID, UID, accepted = line.split(",")
    return MID, 1 if accepted in {"Yes", "Unknown"} else 0

potential_participants_rdd = invitations_rdd.map(parseInvitation)\
    .reduceByKey(lambda a, b: a + b) # pairRDD with (MID, potential_participants)

# join meetings and potential participants, and organize the result
meetings_w_potential_participants_rdd = meetings_rdd\
    .join(potential_participants_rdd)\ # join returns pairRDD with (MID, ((organizer_UID, max_participants), potential_participants))
    .mapValues(lambda values: (values[0][0], values[1], values[0][1]))\ # then we re-organize its values as (MID, (organizer_UID, potential_participants, max_participants))
    .cache() # useful for part 2

# prune meetings within the participants limit, then count the number of exceeding particpants meetings per organizer, and prune those below 15
exceeding_organizers_rdd = meetings_w_potential_participants_rdd\
    .filter(lambda items: items[1][1] > items[1][2])\ # items[1][1] is potential_participants, items[1][2] is max_participants
    .map(lambda items: (items[1][0], 1))\ # we map it to (organizer_UID, 1) for summing
    .reduceByKey(lambda a, b: a+b) \ # this will result in (organizer_UID, num_exceeding_meetings)
    .filter(lambda items: items[1] > 15) \ # if num_exceeding_meetings > 15 then store then keep the organizer UID
    .map(lambda items: items[0]) # This will result in a list of organizer_UIDs

# save the result
exceeding_organizers_rdd.saveAsTextFile(output_path)

# NOTE: .count() is an action for RDDs, but we only call .map(..., 1).reduceByKey(), so the only action here is saving on text file

# ---- OR ----


meetings_df = spark.read.csv(meetings_path, inferSchema=True, header=True)
invitations_df = spark.read.csv(invitations_path, inferSchema=True, header=True)

potential_participants_df = invitations\
    .filter("Accepted = 'Yes' OR Accepted='Unknown'")\
    .groupBy("MID")\
    .count()\ # I suppose count() on all items will return a column named "count(1)"
    .withColumnRenamed("count(1)", "potential_participants") # (MID, potential_participants)

meetings_w_potential_participants_df = meetings_df\
    .join(potential_participants_df, on="MID", how="inner")\ # (MID, organizer_UID, max_participants, potential_participants)
    .cache() # this will be useful in part 2

exceeding_organizers_df = meetings_w_potential_participants_df\
    .filter("potential_participants > max_participants")\
    .groupBy("organizer_UID")\
    .count()\ # I suppose count() on all items will return a column named "count(1)"
    .withColumnRenamed("count(1)", "exceeding_meetings")\ # (organizer_UID, exceeding_meetings)
    .filter("exceeding_meetings > 15")\
    .select("organizer_UID") # This will be the list of organizers UID with more than 15 meetings exceeding the specification

exceeding_organizers_df.write.csv(output_path) # save the result

# NOTE: .count() is NOT an action for DFs if following groupBy(), so the only action here is saving on the file


# ------------------------------------
# Part 2

def parseParticipations(line):
    MID, UID, join, leave = line.split(",")
    return MID, UID

# count actual participants for every meeting, remind however that participations_rdd does not include 0-participation meetings
actual_participants_rdd = participations_rdd\
    .map(parseParticipations).distinct()\ # (MID, UID) , remove repeating UID, MID lines for users who joined the same meeting multiple times
    .mapValues(lambda v: 1)\ # (MID, 1) for each user who participated in MID
    .reduceByKey(lambda a, b: a + b) # (MID, num_of_actual_participants)

# join with the previous rdd, filter on the constraints and count. Then save.
meetings_w_pot_and_act_participants_rdd = meetings_w_potential_participants_rdd\ # (MID, (organizer_UID, potential_participants, max_participants))
    .filter(lambda items: items[1][1] > 10)\ # filter on the constraint potential_participants > 10
    .leftOuterJoin(actual_participants_rdd)\ # (MID, ((organizer_UID, potential_participants, max_participants), actual_participants))
    .mapValues(
        lambda values: (values[0][0], values[0][1], values[0][2], values[1] if values[1] is not None else 0)
    )\  # we re-organize as
        # (MID, (organizer_UID, potential_participants, max_participants, actual_participants))
        # and replace NaNs with 0 for meetings not included in the participants rdd (i.e., those with 0 participants)
    .filter(lambda items: items[1][3] < 2)\ # filter on the constraint actual_participants < 2
    .map(lambda items: (items[1][0], 1))\# (Organizer_UID, 1) for each meeting that the organizer did satisfying the above conditions
    .reduceByKey(lambda a, b: a + b)\ # sum: (organizer_UID, num_meetings_meeting_the_constraint)
    .saveAsTextFile(output_path) # and save


# ---- OR ----


participations_df = spark.read.csv(participations_path, inferSchema=True, header=True)

actual_participants_df = participations_df\
    .select("MID", "UID").distinct()\ # we ignore start and end timestamp, and remove duplicates. Otherwise the same user could be counted twice if it joined and leaved the same meeting
    .groupBy("MID")\
    .count()\
    .withColumnRenamed("count(1)", "actual_participants")\  # (MID, actual_participants)

meetings_w_potential_participants_df\
    .filter("potential_participants > 10")\
    .join(actual_participants_df, on="MID", how="left").fillna(0)\  # (MID, organizer_UID, max_participants, potential_participants, actual_participants)
                                                                    # left outer join because actual_participants_df only contains meetings with at least one participant
                                                                    # we want to store those with zero participants too, so we fill na with 0s
    .filter("actual_participants < 2")\
    .groupBy("organizer_UID")\
    .count()\ # (organizer_UID, num_meetings_meeting_the_constraint)
    .write.csv(output_path)

# NOTE: since we do not fetch data from users.txt, but only meetings, participations and invitations,
#       it is impossible to fetch a user that made no meetings at all, respecting the constraint
#       "select the users who organized at least one meeting..."




















