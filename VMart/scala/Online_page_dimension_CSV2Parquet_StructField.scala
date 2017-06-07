package vmart.parquet

// Generated on: 2017:06:07-17:11:03,167

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Online_page_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Online_page_dimension = StructType(
        StructField(name = "online_page_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "online_page_key").build())
    ::  StructField(name = "start_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "start_date").build())
    ::  StructField(name = "end_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "end_date").build())
    ::  StructField(name = "page_number", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "page_number").build())
    ::  StructField(name = "page_description", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "page_description").build())
    ::  StructField(name = "page_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "page_type").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Online_page_dimension_CSV2Parquet_StructField")
      .getOrCreate()

    val df = sparkSession .read
      .format("com.databricks.spark.csv")
      .option("header", "false") //read the headers
      //      .option("mode", "FAILFAST")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .option("charset", "UTF-8")
      .option("dateFormat", "MM/dd/yyyy")
      .schema(Online_page_dimension)
      .load("/tmp/ros/online_page_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/online_page_dimension.parquet")

  }
}
