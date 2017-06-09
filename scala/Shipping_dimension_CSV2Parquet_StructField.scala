package vmart.parquet

// Generated on: 2017:06:06-15:57:41,755

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Shipping_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Shipping_dimension = StructType(
        StructField(name = "shipping_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "shipping_key").build())
    ::  StructField(name = "ship_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "ship_type").build())
    ::  StructField(name = "ship_mode", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "ship_mode").build())
    ::  StructField(name = "ship_carrier", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "ship_carrier").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Shipping_dimension_CSV2Parquet_StructField")
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
      .schema(Shipping_dimension)
      .load("/tmp/ros/shipping_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/vertica/data/parquet/shipping_dimension.parquet")

  }
}
