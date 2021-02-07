package org.sparkbyexample.sparkscala.dataframe.join

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.plans.Inner

import scala.collection.mutable.ListBuffer

object InnerJoinExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
    (6,"Brown",2,"2010","50","",-1)
  )
  val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","key","gender","salary")
  import spark.sqlContext.implicits._
  val empDF = emp.toDF(empColumns:_*)
  empDF.show(false)

  val dept = Seq(("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  )

  val deptColumns = Seq("department","key")
  val deptDF = dept.toDF(deptColumns:_*)
  deptDF.show(false)

  var colulmns = ListBuffer[Column]()

  empDF.columns.map(empDF(_)).foreach(col =>{
    colulmns += col.alias("source_"+col.toString())
  })
  deptDF.columns.map(deptDF(_)).foreach(col =>
    colulmns += col.alias("dest_"+col.toString()))

  val columns = colulmns.toArray[Column].tail




  println("Inner join")
  val joinDf2 = empDF.join(deptDF,empDF("key") ===  deptDF("key"),"inner").select(columns:_*)

  joinDf2.show()
//  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"))
//    .show(false)
//
//  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),Inner.sql)
//    .show(false)
//
//  empDF.join(deptDF).where(empDF("emp_dept_id") ===  deptDF("dept_id"))
//    .show(false)
//
//  empDF.join(deptDF).filter(empDF("emp_dept_id") ===  deptDF("dept_id"))
//    .show(false)
//
//  empDF.createOrReplaceTempView("EMP")
//  deptDF.createOrReplaceTempView("DEPT")
//
//  val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
//  joinDF2.show(false)
}
