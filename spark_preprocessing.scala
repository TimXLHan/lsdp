import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Load the US and Canadian datasets
val peopleUS = spark.read.format("json").load("/FileStore/tables/10000_random_us_people_profiles.json")
val peopleCA = spark.read.format("json").load("/FileStore/tables/10000_random_canadian_profiles.json")

// Specify the desired columns we want to have for passing on to UniStorage
val columnsToKeep = List(
  "public_identifier", "first_name", "last_name", "full_name", "occupation",
  "headline", "summary", "country", "country_full_name", "city", "state",
  "experiences", "connections", "people_also_viewed"
)

// Select only the columns to keep and filter out rows where "public_identifier" is null for US
val preprocessedDataUS: DataFrame = peopleUS
  .filter(col("public_identifier").isNotNull)
  .select(columnsToKeep.map(col): _*) //  transforms the list of column names into a list of column references that select can use

// Select only the columns to keep and filter out rows where "public_identifier" is null for Canada
val preprocessedDataCA: DataFrame = peopleCA
  .filter(col("public_identifier").isNotNull)
  .select(columnsToKeep.map(col): _*) //  transforms the list of column names into a list of column references that select can use

// Union the US and Canadian datasets
val unionDataAll: DataFrame = preprocessedDataUS.union(preprocessedDataCA)

// Restructure nested column arrays of 'experiences' and 'people_also_viewed' to only contain information needed
val updatedDataFrame = unionDataAll
  .withColumn("experiences", expr("TRANSFORM(experiences, x -> named_struct('starts_at', x.starts_at, 'ends_at', x.ends_at, 'company', x.company, 'company_linkedin_profile_url', x.company_linkedin_profile_url, 'title', x.title, 'description', x.description, 'location', x.location))"))
  .withColumn("people_also_viewed", expr("TRANSFORM(people_also_viewed, x -> named_struct('name', x.name, 'summary', x.summary, 'location', x.location))"))

// Keep only at most three elements for the column arrays of 'experiences' and 'people_also_viewed'
val finalDataFrame = updatedDataFrame
  .withColumn("experiences", slice(col("experiences"), 1, 3))
  .withColumn("people_also_viewed", slice(col("people_also_viewed"), 1, 3))

// Check that schema is to specification
finalDataFrame.printSchema()

// Write output to file, coalesce to ensure the output is in single file
finalDataFrame.coalesce(1).write.json("/FileStore/tables/us_and_canadian_profiles_processed.json")