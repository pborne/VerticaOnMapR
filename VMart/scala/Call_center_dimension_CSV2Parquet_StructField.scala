package vmart.parquet

// Generated on: 2017:06:07-17:11:03,953

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Call_center_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Call_center_dimension = StructType(
        StructField(name = "call_center_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "call_center_key").build())
    ::  StructField(name = "cc_closed_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_closed_date").build())
    ::  StructField(name = "cc_open_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_open_date").build())
    ::  StructField(name = "cc_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_name").build())
    ::  StructField(name = "cc_class", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_class").build())
    ::  StructField(name = "cc_employees", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_employees").build())
    ::  StructField(name = "cc_hours", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_hours").build())
    ::  StructField(name = "cc_manager", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_manager").build())
    ::  StructField(name = "cc_address", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_address").build())
    ::  StructField(name = "cc_city", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_city").build())
    ::  StructField(name = "cc_state", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_state").build())
    ::  StructField(name = "cc_region", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "cc_region").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Call_center_dimension_CSV2Parquet_StructField")
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
      .schema(Call_center_dimension)
      .load("/tmp/ros/call_center_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/call_center_dimension.parquet")

  }
}
