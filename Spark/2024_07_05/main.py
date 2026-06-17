import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

job_postings_path = 'job_postings.txt'
offers_path = 'offers.txt'
contracts_path = 'contracts.txt'

output_path_1 = "path/to/dir1"
output_path_2 = "path/to/dir2"

# ------------------------------------
# Part 1

"""
Job postings with many offers in 2024 and a number of rejected offers greater than
the number of accepted ones in 2024. Considering only the offers made from
January 1, 2024 (referring to OfferDate), the first part of this application selects the
job postings with at least 10 offers (considering all offers in 2024: accepted and
rejected) and a number of rejected offers greater than the number of accepted
offers (always considering only 2024). Store the JobIDs of the selected job postings
and the number of offers for each job posting in 2024 (considering all offers in 2024:
accepted+rejected) in the first output folder. Each output line contains one of the
selected job postings and its number of offers in 2024.
"""

offers = spark.read.csv(offers_path, header=True, inferSchema=True) # (OfferID,JobID,OfferDate,Salary,Status,SSN)

(
    offers
    # Supposing My-SQL syntax, I prefer local SQL functions than udf's to be more optimal
    .filter("STR_TO_DATE(OfferDate, '%Y/%m/%d') >= STR_TO_DATE('2024/01/01', '%Y/%m/%d')")
    .selectExpr("JobID", "CASE WHEN Status = 'Accepted' THEN 1 ELSE 0 END AS Status")
    .groupBy("JobID")
    .agg({"*": "count", "Status": "sum"})
    .withColumnRenamed("count(1)", "NumOffers") # Or whatever this is called, maybe count(*)?
    .withColumnRenamed("sum(Status)", "NumAcceptedOffers")
    .filter("NumOffers >= 10")
    .filter("(NumOffers - NumAcceptedOffers) > NumAcceptedOffers")
    .select("JobID", "NumOffers")
    .write.csv(output_path_1, header=True)
)

# ---- OR ----

# Part 1
def mapJobStatus(l):
    jobID = l.split(",")[1]
    status = l.split(",")[4]

    if (status=='Accepted'):
        return (jobID, (1, -1))
    else:
        return (jobID, (1, +1))

# - filter offers with offer date associated with 2024
# - map to (JobId, (1, +1/-1)) +1 = Rejected, -1 = Accepted
# - reduceByKey to compute the number of offers for each job and the number of rejected offers - number of accepted offers
# - filter #offers>=10 and #rejected>#accepted
# - map to JobId, #offers
# - save
offersPerJob = offers\
    .filter(lambda l: l.split(",")[2].startswith("2024"))\
    .map(mapJobStatus)\
    .reduceByKey(lambda v1, v2: (v1[0]+v2[0], v1[1]+v2[1]))\
    .filter(lambda p: p[1][0]>=10 and p[1][1]>0)\
    .map(lambda p: (p[0], p[1][0]))
# - save the result

offersPerJob.saveAsTextFile(output1)


# ------------------------------------
# Part 2

"""
For each country, select the job titles with a high percentage of accepted offers that
are not associated with signed contracts from the year 2000. The second part of this
application selects for each country the job titles with more than 50% of the
accepted offers not associated with a contract in at least three different years (not
necessarily consecutive), considering the publication date of the job posting
(PublicationDate) as the reference year. Consider only the job postings published
from 2000 for this second part of the task. Store, in the second HDFS output folder,
the selected job titles, the associated countries, and the number of years with more
than 50% of the accepted offers not associated with a contract (one job title,
country, number of years with more than 50% of the accepted offers not associated
with a contract per output line).

+-------------------+---------+------+------+--------+------------+
| Title             | Country | Year |  # A | # A,NC |     %      |
+-------------------+---------+------+------+--------+------------+
| Software Engineer | IT      | 2024 |  10  |    6   |    60%     |
| Software Engineer | IT      | 2023 |  2   |    2   |    100%    |
| Software Engineer | IT      | 2010 |  5   |    4   |    80%     |
| Software Engineer | IT      | 2004 |  1   |    0   |     0%     |
| Data Engineer     | IT      | 2020 |  1   |    1   |    100%    |
| Data Engineer     | IT      | 2019 |  3   |    2   |   66.6%    |
| Data Engineer     | IT      | 2018 |  2   |    2   |    100%    |
| Data Engineer     | ES      | 2018 |  1   |    0   |     0%     |
| Data Scientist    | FR      | 2024 |  8   |    6   |    75%     |
| Data Scientist    | FR      | 2020 |  2   |    2   |    100%    |
| Data Scientist    | FR      | 2018 |  3   |    1   |   33.3%    |
+-------------------+---------+------+------+-----+---------------+

Associated Output:
* Software Engineer, IT, 3
* Data Engineer, IT, 3

"""

job_postings = spark.read.csv(job_postings_path, header=True, inferSchema=True) # (JobID,Title,Country,Continent,PublicationDate)
contracts = spark.read.csv(contracts_path, header=True, inferSchema=True) # (ContractID,OfferID,ContractDate,ContractType)

(
    offers
    .filter("Status = 'Accepted'")
    # Supposing My-SQL syntax, I prefer local SQL functions than udf's to be more optimal
    .join(job_postings.filter("YEAR(STR_TO_DATE(PublicationDate, '%Y/%m/%d')) >= 2000"), on="JobID", how="inner")
    .join(contracts, on="OfferID", how="left")
    #.na.fill(0, ["ContractID"]) # we DONT do this because we want to count ignoring missing ContractID in rows
    .selectExpr(
        "OfferID", "JobID", "ContractID", "YEAR(STR_TO_DATE(PublicationDate, '%Y/%m/%d')) AS Year", "Country", "Title"
    ) # (OfferID, JobID, ContractID{None}, Year, Country, Title)
    .groupBy("Title", "Country", "Year")
    .agg({"*":"count", "ContractID": "count"})
    .withColumnRenamed("count(1)", "NumAcceptedOffers")
    .withColumnRenamed("count(ContractID)", "NumAcceptedOffersWithContract")
    .selectExpr(
        "*", "1 - NumAcceptedOffersWithContract / NumAcceptedOffers AS RatioAcceptedNoContract"
    ) # (Year, Country, Title, NumAcceptedOffers, NumAcceptedOffersWithContract, RatioAcceptedNoContract)
    .selectExpr("*", "CASE WHEN RatioAcceptedNoContract > 0.5 THEN 1 ELSE 0 END AS isBigRatioAcceptedNoContract")
    .groupBy("Country", "Title")
    .agg({"isBigRatioAcceptedNoContract": "sum"})
    .withColumnRenamed("sum(isBigRatioAcceptedNoContract)", "NumYearsWithMoreThan50pAcceptedOffersButNoContract")
    # (Country, Title, NumYearsWithMoreThan50pAcceptedOffersButNoContract)
    .filter("NumYearsWithMoreThan50pAcceptedOffersButNoContract >= 3")
    .select("Title", "Country", "NumYearsWithMoreThan50pAcceptedOffersButNoContract")
    .write.csv(output_path_2)
)

# ---- OR ----

def JobIdTitleCountryYear(l):
    fields = l.split(",")
    jobId=fields[0]
    title=fields[1]
    country=fields[2]
    year=fields[4].split("/")[0]

    return (jobId, (title, country, year))

# - select the JobPosting with year >=2000
# - map to (JobId, (Title, country, year))
jobPostings2000 = jobPostings\
    .filter(lambda l : l.split(',')[4]>='2000/01/01')\
    .map(JobIdTitleCountryYear)

def JobIdOfferId(l):
    fields = l.split(",")
    offerId=fields[0]
    jobId=fields[1]

    return (jobId, offerId)

# - select accepted offers
# - map to (JobId, OfferId)
offersAccepted = offers\
    .filter(lambda l : l.split(',')[4]=='Accepted')\
    .map(JobIdOfferId)

# - join jobPostings2000 with offersAccepted -> (JobId, ((Title, country, year) , OfferId) )
# - map to (OfferId, (Title, country, year))
postingsJoinOffers = jobPostings2000\
    .join(offersAccepted)\
    .map(lambda p : (p[1][1],  p[1][0]))

# - map contracts to (offerId, contractId)
# - contracts right outer join postingsJoinOffers
# - map to ((Title, country, year), (1, 0/1)) 0 = contract - 1 = None, i.e., no contract
# - reduceByKey to compute, for each (Title, country, year), the total number of accepted offers and
#        the accepted offers not associated with a contract
#- select only the pairs with ratio > 50%
titleCountryYear50 = contracts.map(lambda l: (l.split(",")[1], l.split(",")[0]))\
    .rightOuterJoin(postingsJoinOffers)\
    .map(lambda p: (p[1][1], (1, 1 if p[1][0] is None else 0)))\
    .reduceByKey(lambda v1, v2: (v1[0]+v2[0], v1[1]+v2[1]))\
    .filter(lambda p : (p[1][1]/p[1][0])>0.5)

# - map to ((Title, country), 1)
# - reduceByKey to count the number of selected years for each combination (Title, country)
# - select the combination with at least 3 years
titleCountryNumYears = titleCountryYear50.map(lambda p: ( (p[0][0],p[0][1]), 1) )\
    .reduceByKey(lambda v1, v2: v1+v2)\
    .filter(lambda p: p[1]>=3)

# Save the result
titleCountryNumYears.saveAsTextFile(output2)














