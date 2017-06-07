package vmart.parquet

// Generated on: 2017:06:07-17:11:04,337

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Warehouse_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Warehouse_dimension = StructType(
        StructField(name = "warehouse_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "warehouse_key").build())
    ::  StructField(name = "warehouse_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "warehouse_name").build())
    ::  StructField(name = "warehouse_address", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "warehouse_address").build())
    ::  StructField(name = "warehouse_city", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "warehouse_city").build())
    ::  StructField(name = "warehouse_state", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "warehouse_state").build())
    ::  StructField(name = "warehouse_region", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "warehouse_region").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Warehouse_dimension_CSV2Parquet_StructField")
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
      .schema(Warehouse_dimension)
      .load("/tmp/ros/warehouse_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/warehouse_dimension.parquet")

  }
}
