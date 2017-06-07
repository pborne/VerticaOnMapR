package vmart.parquet

// Generated on: 2017:06:07-17:11:03,560

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Online_sales_fact_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Online_sales_fact = StructType(
        StructField(name = "sale_date_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "sale_date_key").build())
    ::  StructField(name = "ship_date_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "ship_date_key").build())
    ::  StructField(name = "product_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_key").build())
    ::  StructField(name = "product_version", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_version").build())
    ::  StructField(name = "customer_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "customer_key").build())
    ::  StructField(name = "call_center_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "call_center_key").build())
    ::  StructField(name = "online_page_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "online_page_key").build())
    ::  StructField(name = "shipping_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "shipping_key").build())
    ::  StructField(name = "warehouse_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "warehouse_key").build())
    ::  StructField(name = "promotion_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "promotion_key").build())
    ::  StructField(name = "pos_transaction_number", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "pos_transaction_number").build())
    ::  StructField(name = "sales_quantity", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "sales_quantity").build())
    ::  StructField(name = "sales_dollar_amount", dataType = FloatType, nullable = true, metadata = new MetadataBuilder().putString("description", "sales_dollar_amount").build())
    ::  StructField(name = "ship_dollar_amount", dataType = FloatType, nullable = true, metadata = new MetadataBuilder().putString("description", "ship_dollar_amount").build())
    ::  StructField(name = "net_dollar_amount", dataType = FloatType, nullable = true, metadata = new MetadataBuilder().putString("description", "net_dollar_amount").build())
    ::  StructField(name = "cost_dollar_amount", dataType = FloatType, nullable = true, metadata = new MetadataBuilder().putString("description", "cost_dollar_amount").build())
    ::  StructField(name = "gross_profit_dollar_amount", dataType = FloatType, nullable = true, metadata = new MetadataBuilder().putString("description", "gross_profit_dollar_amount").build())
    ::  StructField(name = "transaction_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "transaction_type").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Online_sales_fact_CSV2Parquet_StructField")
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
      .schema(Online_sales_fact)
      .load("/tmp/ros/online_sales_fact.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/online_sales_fact.parquet")

  }
}
