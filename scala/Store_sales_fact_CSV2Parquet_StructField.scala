package vmart.parquet

// Generated on: 2017:06:06-15:57:42,250

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Store_sales_fact_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Store_sales_fact = StructType(
        StructField(name = "date_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "date_key").build())
    ::  StructField(name = "product_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_key").build())
    ::  StructField(name = "product_version", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_version").build())
    ::  StructField(name = "store_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "store_key").build())
    ::  StructField(name = "promotion_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "promotion_key").build())
    ::  StructField(name = "customer_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "customer_key").build())
    ::  StructField(name = "employee_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "employee_key").build())
    ::  StructField(name = "pos_transaction_number", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "pos_transaction_number").build())
    ::  StructField(name = "sales_quantity", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "sales_quantity").build())
    ::  StructField(name = "sales_dollar_amount", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "sales_dollar_amount").build())
    ::  StructField(name = "cost_dollar_amount", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "cost_dollar_amount").build())
    ::  StructField(name = "gross_profit_dollar_amount", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "gross_profit_dollar_amount").build())
    ::  StructField(name = "transaction_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "transaction_type").build())
    ::  StructField(name = "transaction_time", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "transaction_time").build())
    ::  StructField(name = "tender_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "tender_type").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Store_sales_fact_CSV2Parquet_StructField")
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
      .schema(Store_sales_fact)
      .load("/tmp/ros/store_sales_fact.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/vertica/data/parquet/store_sales_fact.parquet")

  }
}
