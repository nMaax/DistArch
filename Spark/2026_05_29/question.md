# Spark
PoliMeeting is an international company that manages online meetings around the world. Statistics about the organized meetings and users are computed based on the following input data files, which have been collected in the company's latest 15 years of activity.

* Users.txt
  + Users.txt is a textual file containing information about the users who organized or participated in meetings managed by PoliMeeting. There is one line for each user and the total number of users is greater than 150,000,000. This file is large and you cannot suppose the content of Users.txt can be stored in one in-memory variable.
  + Each line of Users.txt has the following format
    - UID,Name,Surname,Country,PricingPlan

where *UID* is the user’s unique identifier, *Name* and *Surname* are his/her name and surname, respectively, *Country* is the country where he/she lives, and *PricingPlan* is the type of pricing plan (free, business, etc.).

* + - For example, the following line

*User1000,Mario,Rossi,Italian,Business*

means that the name and surname of the user with identifier User1000 are Mario and Rossi, respectively, and he is Italian. He has subscribed to a Business pricing plan.

* Meetings.txt
  + Meetings.txt is a textual file containing information about the events managed by PoliMeeting. There is one line for each meeting. The total number of meetings stored into Meetings.txt is greater than 2,000,000,000. This file is large and you cannot suppose the content of Meetings.txt can be stored in one in-memory variable.
  + Each line of Meetings.txt has the following format
    - MID,Title,StartTime,EndTime,OrganizerUID,MaxParticipants

where *MID* is the item unique identifier, *Title* is the title of the meeting, *StartTime* is the start time of the meeting, *EndTime* is the end time of the meeting, *OrganizerUID* is the identifier of the user who organized the meeting and *MaxParticipants* is the maximum number of allowed participants.

StartTime and EndTime are timestamps in the format YYYY/MM/DD-HH:MM:SS.

* + - For example, the following line

*MID1034,Polito project kick-off,2024/02/07-20:30:00,2024/02/07-21:30:00,User1000,20*

means that the meeting with MID ***MID1034*** was organized by User1000, is titled **“*Polito project kick-off”***, and the maximum number of allowed participants is ***20***. The meeting was scheduled from ***2024/02/07-20:30:00*** to ***2024/02/07-21:30:00***.

* Invitations.txt
  + Invitations.txt is a textual file containing information about invitations to meetings. A new line is inserted in Invitations.txt every time someone is invited to a meeting. Invitations.txt includes the historical data about the latest 15 years. This file is big and you cannot suppose the content of Purchases.txt can be stored in one in-memory variable.
  + Each line of Invitations.txt has the following format
    - MID,UID,Accepted

where MID is the identifier of the meeting to which user UID has been invited. Accepted can assume three values: Yes, No, and Unknown, depending on the answer of the invited user.

* + - For example, the following line

*MID1034,User1000,Yes*

means that ***User1000*** has been invited to the meeting ***MID1034***, and he/she has accepted the invitation to participate.

Note that the same user can be invited to many meetings, and each meeting can have many invited users. Each combination (MID, UID) occurs at most one time in Invitations.txt.

* Participations.txt
  + Participations.txt is a textual file containing information about who participated in the organized meetings. A new line is inserted in Participations.txt every time someone joins (participates in) a meeting. Participations.txt includes the historical data about the latest 15 years. This file is big and you cannot suppose the content of Participations.txt can be stored in one in-memory variable.
  + Each line of Participations.txt has the following format
    - MID,UID,JoinTimestamp,LeaveTimestamp

where MID is the identifier of the meeting that user UID joined at *JoinTimestamp*. *LeaveTimestamp* is the timestamp at which UID left the meeting MID. The format of the timestamps *JoinTimestamp* and *LeaveTimestamp* is YYYY/MM/DD-HH:MM:SS.

* + - For example, the following line

*MID1034,User10,2024/02/07-20:40:10, 2024/02/07-20:50:02*

means that ***User10*** joined the meeting ***MID1034*** on ***July 2, 2024***, at ***20:40:10*** and left it on ***July 2, 2024***, at ***20:50:02***.

Note that the same user can participate in many meetings, and each meeting can have many participants. Moreover, **the same user can join and leave each meeting several times** (a new line associated with a different JoinTimestamp is inserted every time a user joins or rejoins the same meeting). Each triplet (MID, UID, JoinTimestamp) occurs at most one time in Participations.txt.

## Exam Question
The managers of PoliMeeting asked you to develop a single Spark-based application based either on RDDs or Spark SQL to address the following tasks. The application takes the paths of the input files and two output folders (associated with the outputs of the following points 1 and 2, respectively).

1. *Users who frequently organize meetings with too many expected participants.* The first part of this application selects the users who frequently organize meetings with too many expected participants. Specifically, a user is selected if more than 15 of the meetings the user organized are characterized by a number of potential participants greater than the maximum number of allowed participants. A user is considered a *potential participant* in a meeting if he/she answers ‘Yes’ or ‘Unknown’ to the invitation to that meeting (i.e., Accept=’Yes’ or Accept=’Unknown’). Store the identifiers (UIDs) of the selected users in the first HDFS output folder. Specifically, store one UID per output line.
2. *The number of meetings with many potential participants and a few actual participants organized by each user.* The second part of this application considers only the users who organized at least one meeting and computes for each of them the number of organized meetings characterized by less than 2 unique actual participants (i.e., no users or at most one user actually participated in the meeting) and more than 10 potential participants (the definition of potential participant is the same reported in the first part). Store the result in the second HDFS output folder. Specifically, there is one output line for each user who organized at least one meeting and the number of meetings organized by that user that satisfy the conditions reported in this second part of the problem specification. Those users who organized zero meetings that satisfy the conditions of interest **are not part** of the result.

Output format of each output line (second part):

*OrganizerUID, Number of meetings characterized by less than 2 unique actual participants and more than 10 potential participants organized by OrganizerUID.*

**Note**. Remind that the same user can participate multiple times in the same meeting.

### Example for the second part.

In this small example, suppose there are only three users who organized meetings. The identifiers of these users are UID1, UID5, and UID12.

Suppose that UID1 organized three meetings

* A first meeting with 3 unique actual participants and 15 potential participants
* A second meeting with **0** actual participants and **12** potential participants **(i.e., actual participants<2 and potential participants>10)**
* A third meeting with 0 unique actual participants and 5 potential participants

Suppose that UID5 organized four meetings

* A first meeting with 10 unique actual participants and 15 potential participants
* A second meeting with **0** unique actual participants and **15** potential participants **(i.e., actual participants<2 and potential participants>10)**
* A third meeting with 1 unique actual participant and 5 potential participants
* A fourth meeting with **1** unique actual participant and **12** potential participants **(i.e., actual participants<2 and potential participants>10)**

Suppose that UID12 organized four meetings

* A first meeting with 10 unique actual participants and 15 potential participants
* A second meeting with 5 unique actual participants and 11 potential participants
* A third meeting with 0 unique actual participants and 5 potential participants
* A fourth meeting with 5 unique actual participants and 6 potential participants

The second output folder must contain the following two lines:

* UID1,1
* UID5,2

Note that UID12 is not part of the result because UID12 organized some meetings, but none of those meetings satisfy the conditions specified in the second part of the problem.

* You do not need to write imports. Focus on the content of the main method.
* **Only if you use Spark SQL**, suppose the first line of each file contains the header information/the name of the attributes. Suppose, instead, there are no header lines if you use RDDs.
* Suppose both Spark Context ***sc*** and SparkSession ***spark*** have already been set.
* Please **comment** your solution by stating the meaning of the fields you intend to process with each instruction, e.g., key=(product id, date), value=(category, year)

