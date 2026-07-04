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

# --- RDDs ---

offers = sc.textFile(offers_path) # (OfferID,JobID,OfferDate,Salary,Status,SSN)

(
    offers
    .map(lambda line: line.split(","))
    .filter(lambda items: int(items[2][:4]) >= 2024) # offers made from January 1, 2024
    # NOTE: alternatively you can reduce upone two columns only, considering the other as a subtraction of it
    #       e.g. totOffers - totAccepted = totRejected
    #       another approach is to use two columns, one for totOffers and the other as a cumuluative
    #       sum of +1 if accepted and -1 if rejected, if the final result is < 0 then we keep it
    .map(lambda items: (items[1], (int(items[3] == 'Accepted'), int(items[3] == 'Rejected') , 1))) # JobID, (isAccepted{0,1}, isRejected{0,1}, 1)
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) # JobID, (totAccepted, totRejected, totOffers)
    .filter(lambda pair: pair[1][1] > pair[1][0], pair[1][2] > 10) # totRejected > totAccepted, and totOffers > 10
    .map(lambda pair: f"{pair[0]},{pair[1][2]}") # "JobID,totOffers"
    .saveAsTextFile(output_path_1)
)


# --- DataFrames ---

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
| Title             | Country | Year | # A  | # A,NC |     %      | Include?
+-------------------+---------+------+------+--------+------------+
| Software Engineer | IT      | 2024 |  10  |    6   |    60%     | V
| Software Engineer | IT      | 2023 |  2   |    2   |    100%    | V
| Software Engineer | IT      | 2010 |  5   |    4   |    80%     | V
| Software Engineer | IT      | 2004 |  1   |    0   |     0%     | X (tot = 3)
| Data Engineer     | IT      | 2020 |  1   |    1   |    100%    | V
| Data Engineer     | IT      | 2019 |  3   |    2   |   66.6%    | V
| Data Engineer     | IT      | 2018 |  2   |    2   |    100%    | V
| Data Engineer     | ES      | 2018 |  1   |    0   |     0%     | X (tot = 3)
| Data Scientist    | FR      | 2024 |  8   |    6   |    75%     | V
| Data Scientist    | FR      | 2020 |  2   |    2   |    100%    | V
| Data Scientist    | FR      | 2018 |  3   |    1   |   33.3%    | X (tot = 2)
+-------------------+---------+------+------+-----+---------------+

Associated Output:
- Software Engineer, IT, 3
- Data Engineer, IT, 3

Note that there are from 0 to 1 contracts for each accepted offer. No
contracts for rejected offers.
"""

# --- RDDs ---


job_postings = sc.textFile(job_postings_path) # (JobID,Title,Country,Continent,PublicationDate)
contracts = sc.textFile(contracts_path) # (ContractID,OfferID,ContractDate,ContractType)

accepted_offers = (
    offers
    .map(lambda line: line.split(","))
    .filter(lambda items: items[4] == 'Accepted') # Only Accepted offers
    .map(lambda items: (items[1], items[0])) # JobID, OfferID
)

job_posting_since_2000 = (
    job_postings
    .map(lambda line: line.split(","))
    .filter(lambda items: int(items[4][:4]) >= 2000) # Only Job postings since 2000
    .map(lambda items: (items[0], (items[1], items[2], int(items[4][:4])))) # JobID, (Title, Country, PublicationYear)
)

accepted_offers_since_2000 = (
    accepted_offers
    .join(job_posting_since_2000) # JobID, (OfferID, (Title, Country, PublicationYear))
    .map(lambda pair: (pair[1][0], (pair[1][1][0], pair[1][1][1], pair[1][1][2]))) # OfferID, (Title, Country, PublicationYear)
)

# NOTE: since there are from 0 to 1 contract for each accepted offer, and no contract for rejected offers
# OfferID is functionally unique in contracts table:
#   if to some contract CA there is associated some offer OA, then offer OB cannot be associated to CA, and OA must be exclusively accepted
#   similarly, we suppose a given offer OA cannot be associated to multiple other contract (e.g. OA -> CA, CB)
#   if not, then we can simply add a distinct() below the map to (OfferId, 1)
contracts_simple = (
    contracts
    .map(lambda line: line.split(","))
    .map(lambda items: (items[1], 1)) # OfferID, 1
)

tot_accepted_offers = (
    accepted_offers_since_2000
    .leftOuterJoin(contracts_simple) # OfferID, ((Title, Country, PublicationYear), 1 or None)
    .map(lambda pair: ((pair[1][0][0], pair[1][0][1], pair[1][0][2]), (0 if pair[1][1] is None else 1, 1))) # (Title, Country, PublicationYear), (AssociatedWithContract{0,1}, 1)
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) # (Title, Country, PublicationYear), (TotAcceptedOffersWithContract, TotAcceptedOffers)
)

(
    tot_accepted_offers
    .map(lambda pair: ((pair[0][0], pair[0][1]), 1 if 1 - pair[1][0] / pair[1][1] > 0.5 else 0)) # (Title, Country), ManyNoContractAcceptedOffers{0, 1} (over different years)
    # ManyNoContractAcceptedOffers is obtained by (TotAcceptedOffers - TotAcceptedOffersWithContract)/TotAcceptedOffers = TotAcceptedOffersNoContract/TotAcceptedOffers > 0.5 (or, equivalently 1 - TotAcceptedOffersWithContract/TotAcceptedOffers > 0.5)
    # NOTE: Keep only years with ManyNoContractAcceptedOffers, 0's do not serve us, so we can do a faster reduceByKey
    .filter(lambda pair: pair[1] == 1)
    .reduceByKey(lambda a, b: a+b) # (Title, Country), totYearsWithManyNoContractAcceptedOffers
    .filter(lambda pair: pair[1]>=3)
    .map(lambda pair: f"{pair[0][0]}, {pair[0][1]}, {pair[1]}")
    .saveAsTextFile(output_path_2)
)


# --- DataFrames ---

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

# --- SparkSQL ---

# Register base views
offers.createOrReplaceTempView("offers_v")
job_postings.createOrReplaceTempView("postings_v")
contracts.createOrReplaceTempView("contracts_v")

pure_sql_query = """
    WITH YearlyMetrics AS (
        -- Step 1: Calculate the total counts and ratios per Title, Country, and Year
        SELECT
            p.Title,
            p.Country,
            YEAR(TO_DATE(p.PublicationDate, 'yyyy/MM/dd')) as PubYear,
            COUNT(*) as TotalAccepted,
            COUNT(c.ContractID) as TotalContracts
        FROM offers_v o
        INNER JOIN postings_v p ON o.JobID = p.JobID
        LEFT JOIN contracts_v c ON o.OfferID = c.OfferID
        WHERE o.Status = 'Accepted'
          AND YEAR(TO_DATE(p.PublicationDate, 'yyyy/MM/dd')) >= 2000
        GROUP BY p.Title, p.Country, YEAR(TO_DATE(p.PublicationDate, 'yyyy/MM/dd'))
    ),
    FlaggedYears AS (
        -- Step 2: Identify individual years where no-contract ratio is > 50%
        SELECT
            Title,
            Country,
            CASE WHEN (1.0 - (TotalContracts / TotalAccepted)) > 0.5 THEN 1 ELSE 0 END as IsTargetYear
        FROM YearlyMetrics
    )
    -- Step 3: Roll up metrics by Title and Country to count the target years
    SELECT
        Title,
        Country,
        SUM(IsTargetYear) as TargetYearCount
    FROM FlaggedYears
    GROUP BY Title, Country
    HAVING SUM(IsTargetYear) >= 3
"""

# Run query and save
spark.sql(pure_sql_query).write.csv(output_path_2, header=True)













