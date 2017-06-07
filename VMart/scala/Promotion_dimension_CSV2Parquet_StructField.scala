package vmart.parquet

// Generated on: 2017:06:07-17:11:02,931

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Promotion_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Promotion_dimension = StructType(
        StructField(name = "promotion_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "promotion_key").build())
    ::  StructField(name = "promotion_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "promotion_name").build())
    ::  StructField(name = "price_reduction_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "price_reduction_type").build())
    ::  StructField(name = "promotion_media_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "promotion_media_type").build())
    ::  StructField(name = "ad_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "ad_type").build())
    ::  StructField(name = "display_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "display_type").build())
    ::  StructField(name = "coupon_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "coupon_type").build())
    ::  StructField(name = "ad_media_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "ad_media_name").build())
    ::  StructField(name = "display_provider", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "display_provider").build())
    ::  StructField(name = "promotion_cost", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "promotion_cost").build())
    ::  StructField(name = "promotion_begin_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "promotion_begin_date").build())
    ::  StructField(name = "promotion_end_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "promotion_end_date").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Promotion_dimension_CSV2Parquet_StructField")
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
      .schema(Promotion_dimension)
      .load("/tmp/ros/promotion_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/promotion_dimension.parquet")

  }
}
