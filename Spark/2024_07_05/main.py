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

offers = spark.read.csv(offers_path, header=True, inferSchema=True)

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

job_postings = spark.read.csv(job_postings_path, header=True, inferSchema=True)
contracts = spark.read.csv(contracts_path, header=True, inferSchema=True)

accepted_offers = (
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














