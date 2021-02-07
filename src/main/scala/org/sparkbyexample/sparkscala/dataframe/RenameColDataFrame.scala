package org.sparkbyexample.sparkscala.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{col, _}

object RenameColDataFrame {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

    df.printSchema()

    df.withColumnRenamed("dob","DateOfBirth")
        .printSchema()

    val schema2 = new StructType()
        .add("fname",StringType)
        .add("middlename",StringType)
        .add("lname",StringType)

    df.select(col("name").cast(schema2),
      col("dob"),
      col("gender"),
      col("salary"))
        .printSchema()

    df.select(col("name.firstname").as("fname"),
      col("name.middlename").as("mname"),
      col("name.lastname").as("lname"),
      col("dob"),col("gender"),col("salary"))
      .printSchema()

    df.withColumnRenamed("name.firstname","fname")
      .withColumnRenamed("name.middlename","mname")
      .withColumnRenamed("name.lastname","lname")
      .drop("name")
      .printSchema()




    val old_columns = Seq("dob","gender","salary","fname","mname","lname")
    val new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
    val columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
    val df5 = df.select(columnsList:_*)
    df5.printSchema()

    val newColumns = Seq("newCol1","newCol2","newCol3")
    val df6 = df5.toDF(newColumns:_*)

    val old_columns1 = Seq("dob","gender","salary","fname","mname","lname")
    val new_columns1 = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
    val columnsList1 = old_columns1.zip(new_columns1).map(f=>{col(f._1).as(f._2)})
    val df7 = df5.select(columnsList1:_*)
    df7.printSchema()


  }
}
