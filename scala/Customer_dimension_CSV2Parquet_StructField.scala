package vmart.parquet

// Generated on: 2017:06:06-15:57:41,439

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Customer_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Customer_dimension = StructType(
        StructField(name = "customer_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "customer_key").build())
    ::  StructField(name = "customer_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_type").build())
    ::  StructField(name = "customer_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_name").build())
    ::  StructField(name = "customer_gender", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_gender").build())
    ::  StructField(name = "title", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "title").build())
    ::  StructField(name = "household_id", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "household_id").build())
    ::  StructField(name = "customer_address", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_address").build())
    ::  StructField(name = "customer_city", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_city").build())
    ::  StructField(name = "customer_state", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_state").build())
    ::  StructField(name = "customer_region", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_region").build())
    ::  StructField(name = "marital_status", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "marital_status").build())
    ::  StructField(name = "customer_age", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_age").build())
    ::  StructField(name = "number_of_children", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "number_of_children").build())
    ::  StructField(name = "annual_income", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "annual_income").build())
    ::  StructField(name = "occupation", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "occupation").build())
    ::  StructField(name = "largest_bill_amount", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "largest_bill_amount").build())
    ::  StructField(name = "store_membership_card", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "store_membership_card").build())
    ::  StructField(name = "customer_since", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "customer_since").build())
    ::  StructField(name = "deal_stage", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "deal_stage").build())
    ::  StructField(name = "deal_size", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "deal_size").build())
    ::  StructField(name = "last_deal_update", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "last_deal_update").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Customer_dimension_CSV2Parquet_StructField")
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
      .schema(Customer_dimension)
      .load("/tmp/ros/customer_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/vertica/data/parquet/customer_dimension.parquet")

  }
}
