package vmart.parquet

// Generated on: 2017:06:06-15:57:41,986

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Product_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Product_dimension = StructType(
        StructField(name = "product_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_key").build())
    ::  StructField(name = "product_version", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "product_version").build())
    ::  StructField(name = "product_description", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "product_description").build())
    ::  StructField(name = "sku_number", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "sku_number").build())
    ::  StructField(name = "category_description", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "category_description").build())
    ::  StructField(name = "department_description", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "department_description").build())
    ::  StructField(name = "package_type_description", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "package_type_description").build())
    ::  StructField(name = "package_size", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "package_size").build())
    ::  StructField(name = "fat_content", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "fat_content").build())
    ::  StructField(name = "diet_type", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "diet_type").build())
    ::  StructField(name = "weight", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "weight").build())
    ::  StructField(name = "weight_units_of_measure", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "weight_units_of_measure").build())
    ::  StructField(name = "shelf_width", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "shelf_width").build())
    ::  StructField(name = "shelf_height", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "shelf_height").build())
    ::  StructField(name = "shelf_depth", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "shelf_depth").build())
    ::  StructField(name = "product_price", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "product_price").build())
    ::  StructField(name = "product_cost", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "product_cost").build())
    ::  StructField(name = "lowest_competitor_price", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "lowest_competitor_price").build())
    ::  StructField(name = "highest_competitor_price", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "highest_competitor_price").build())
    ::  StructField(name = "average_competitor_price", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "average_competitor_price").build())
    ::  StructField(name = "discontinued_flag", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "discontinued_flag").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Product_dimension_CSV2Parquet_StructField")
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
      .schema(Product_dimension)
      .load("/tmp/ros/product_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/vertica/data/parquet/product_dimension.parquet")

  }
}
