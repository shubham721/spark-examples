package org.apache.spark.goyal.examples.sql.hive

import org.apache.spark.sql.SparkSession

object ExperimentHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select * from test.orders")

  }

}
