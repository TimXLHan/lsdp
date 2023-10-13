import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{SQLTransformer, Imputer, VectorAssembler}

// Don't know how the data is formatted so I created a generall preprocessing model we can maybe use. One method involves using a pipeline and the other uses spark SQL to achieve the same thing.
// Can customize the preprocessing more when I know how the data is structured and what flaws it might have.

// creating dataframe retailData
val retailData = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/FileStore/tables/data_2010_12_03.csv")

retailData.printSchema()

// Preprocessing
// Define a SQLTransformer to filter out rows where CustomerID is not null
val filterTransformer = new SQLTransformer()
  .setStatement("SELECT * FROM __THIS__ WHERE CustomerID IS NOT NULL")

// Define an Imputer to handle missing values (if any) in "Quantity" and "unitPrice" columns
val imputer = new Imputer()
  .setInputCols(Array("Quantity", "UnitPrice"))
  .setOutputCols(Array("Quantity_imputed", "UnitPrice_imputed"))
  .setStrategy("mean")

// Create a pipeline with the defined stages
val pipeline = new Pipeline()
  .setStages(Array(filterTransformer, imputer))

// Fit the pipeline to the data
val preprocessedModel = pipeline.fit(retailData)

// Transform the data
val preprocessedDataFrame = preprocessedModel.transform(retailData)

// Show the preprocessed data
preprocessedDataFrame.limit(5).show()

// Here is an alternative approach where we use spark SQL instead to preprocess the data in the same way

// Register the retailData DataFrame as a temporary SQL table
retailData.createOrReplaceTempView("retail_data")

// Use Spark SQL to filter rows where CustomerID is not null
val filteredDataFrame = spark.sql("SELECT * FROM retail_data WHERE CustomerID IS NOT NULL")

// Calculate the mean of Quantity and UnitPrice
val meanQuantity = filteredDataFrame.selectExpr("avg(Quantity)").first()(0).asInstanceOf[Double]
val meanUnitPrice = filteredDataFrame.selectExpr("avg(UnitPrice)").first()(0).asInstanceOf[Double]

// Use Spark SQL to replace NaN values with the mean
val preprocessedDataFrame2 = filteredDataFrame
  .na.fill(meanQuantity, Seq("Quantity"))
  .na.fill(meanUnitPrice, Seq("UnitPrice"))

// Show the preprocessed data
preprocessedDataFrame2.limit(5).show()


