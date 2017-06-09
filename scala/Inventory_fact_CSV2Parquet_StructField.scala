package vmart.parquet

// Generated on: 2017:06:06-15:57:41,790

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Inventory_fact_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Inventory_fact = StructType(
        StructField(name = "date_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "date_key").build())
    ::  StructField(name = "product_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_key").build())
    ::  StructField(name = "product_version", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_version").build())
    ::  StructField(name = "warehouse_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "warehouse_key").build())
    ::  StructField(name = "qty_in_stock", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "qty_in_stock").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Inventory_fact_CSV2Parquet_StructField")
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
      .schema(Inventory_fact)
      .load("/tmp/ros/inventory_fact.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/vertica/data/parquet/inventory_fact.parquet")

  }
}
