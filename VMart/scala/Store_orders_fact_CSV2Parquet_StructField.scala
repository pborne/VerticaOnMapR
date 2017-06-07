package vmart.parquet

// Generated on: 2017:06:07-17:11:03,807

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Store_orders_fact_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Store_orders_fact = StructType(
        StructField(name = "product_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_key").build())
    ::  StructField(name = "product_version", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_version").build())
    ::  StructField(name = "store_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "store_key").build())
    ::  StructField(name = "vendor_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "vendor_key").build())
    ::  StructField(name = "employee_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "employee_key").build())
    ::  StructField(name = "order_number", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "order_number").build())
    ::  StructField(name = "date_ordered", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "date_ordered").build())
    ::  StructField(name = "date_shipped", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "date_shipped").build())
    ::  StructField(name = "expected_delivery_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "expected_delivery_date").build())
    ::  StructField(name = "date_delivered", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "date_delivered").build())
    ::  StructField(name = "quantity_ordered", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "quantity_ordered").build())
    ::  StructField(name = "quantity_delivered", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "quantity_delivered").build())
    ::  StructField(name = "shipper_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "shipper_name").build())
    ::  StructField(name = "unit_price", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "unit_price").build())
    ::  StructField(name = "shipping_cost", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "shipping_cost").build())
    ::  StructField(name = "total_order_cost", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "total_order_cost").build())
    ::  StructField(name = "quantity_in_stock", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "quantity_in_stock").build())
    ::  StructField(name = "reorder_level", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "reorder_level").build())
    ::  StructField(name = "overstock_ceiling", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "overstock_ceiling").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Store_orders_fact_CSV2Parquet_StructField")
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
      .schema(Store_orders_fact)
      .load("/tmp/ros/store_orders_fact.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/store_orders_fact.parquet")

  }
}
