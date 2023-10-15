import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{SQLTransformer, Imputer, VectorAssembler}

// Load the US and Canadian datasets from LinkedIn, check schemas and perform a query
val peopleUS = spark.read.format("json").load("/FileStore/tables/10000_random_us_people_profiles.json")
val peopleCA = spark.read.format("json").load("/FileStore/tables/10000_random_canadian_profiles.json")

//peopleUS.printSchema()
//peopleCA.printSchema()

// Register the people tables as temporary SQL tables
peopleUS.createOrReplaceTempView("people_US")
peopleCA.createOrReplaceTempView("people_CA")

// Use Spark SQL to filter rows where connections is not null
val filteredDataFrameUS = spark.sql("SELECT * FROM people_US WHERE connections IS NOT NULL")
val filteredDataFrameCA = spark.sql("SELECT * FROM people_CA WHERE connections IS NOT NULL")

// Check the mean number of connections
val meanConnectionsUS = filteredDataFrameUS.selectExpr("avg(connections)").first()(0).asInstanceOf[Double]
val meanConnectionsCA = filteredDataFrameCA.selectExpr("avg(connections)").first()(0).asInstanceOf[Double]

println(s"Mean connections in the US: $meanConnectionsUS and in Canada: $meanConnectionsCA")
