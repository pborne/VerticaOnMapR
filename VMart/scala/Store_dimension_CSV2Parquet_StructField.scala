package vmart.parquet

// Generated on: 2017:06:07-17:11:03,906

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Store_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Store_dimension = StructType(
        StructField(name = "store_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "store_key").build())
    ::  StructField(name = "store_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "store_name").build())
    ::  StructField(name = "store_number", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "store_number").build())
    ::  StructField(name = "store_address", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "store_address").build())
    ::  StructField(name = "store_city", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "store_city").build())
    ::  StructField(name = "store_state", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "store_state").build())
    ::  StructField(name = "store_region", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "store_region").build())
    ::  StructField(name = "floor_plan_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "floor_plan_type").build())
    ::  StructField(name = "photo_processing_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "photo_processing_type").build())
    ::  StructField(name = "financial_service_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "financial_service_type").build())
    ::  StructField(name = "selling_square_footage", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "selling_square_footage").build())
    ::  StructField(name = "total_square_footage", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "total_square_footage").build())
    ::  StructField(name = "first_open_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "first_open_date").build())
    ::  StructField(name = "last_remodel_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "last_remodel_date").build())
    ::  StructField(name = "number_of_employees", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "number_of_employees").build())
    ::  StructField(name = "annual_shrinkage", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "annual_shrinkage").build())
    ::  StructField(name = "foot_traffic", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "foot_traffic").build())
    ::  StructField(name = "monthly_rent_cost", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "monthly_rent_cost").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Store_dimension_CSV2Parquet_StructField")
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
      .schema(Store_dimension)
      .load("/tmp/ros/store_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/store_dimension.parquet")

  }
}
