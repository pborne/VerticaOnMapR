package vmart.parquet

// Generated on: 2017:06:06-15:57:41,780

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Vendor_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Vendor_dimension = StructType(
        StructField(name = "vendor_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "vendor_key").build())
    ::  StructField(name = "vendor_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "vendor_name").build())
    ::  StructField(name = "vendor_address", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "vendor_address").build())
    ::  StructField(name = "vendor_city", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "vendor_city").build())
    ::  StructField(name = "vendor_state", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "vendor_state").build())
    ::  StructField(name = "vendor_region", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "vendor_region").build())
    ::  StructField(name = "deal_size", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "deal_size").build())
    ::  StructField(name = "last_deal_update", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "last_deal_update").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Vendor_dimension_CSV2Parquet_StructField")
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
      .schema(Vendor_dimension)
      .load("/tmp/ros/vendor_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/vertica/data/parquet/vendor_dimension.parquet")

  }
}
