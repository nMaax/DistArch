# MapReduce - Hadoop
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

## Question
The managers of PoliMeeting are interested in performing some analyses about the pricing plans.

Design a single application, based on MapReduce and Hadoop, and write the corresponding Java code, to address the following point:

1. *Countries where all users have a free pricing plan.* The application selects the countries where 100% of their users have a free pricing plan (PrincingPlan=‘free’). The selected countries and the number of users for each of those countries are stored in the output HDFS folder.

Output format (one line per each selected country):

*country, number of users for that country*

Suppose that the input is Users.txt and has been already set. Suppose that also the name of the output folder has been already set.

* Write only the content of the Mapper and Reducer classes (map and reduce methods. setup and cleanup if needed). The content of the Driver must not be reported.
* Use the following two specific multiple-choice questions (**Exercises 1.2 and 1.3**) to specify the number of instances of the reducer class for each job.
* If you need personalized classes, report for each of them:
  + the name of the class
  + attributes/fields of the class (data type and name)
  + personalized methods (if any), e.g., the content of the toString() method if you override it
  + do not report the get and set methods. Suppose they are "automatically defined"