package com.code.decre.spark.rdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author liaofa
  * @date 2019/9/11
  */
object TestDataFrame2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDataFrame2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val fileRDD = sc.textFile("D:\\dragonsoft\\test-txt\\people.txt")

    val rowRDD: RDD[Row] = fileRDD.map(line => {
      val fields = line.split(",")
      Row(fields(0), fields(1).trim.toInt)
    })

    val structType: StructType = StructType(
      StructField("name", StringType, true) ::
      StructField("age", IntegerType, true) :: Nil
    )

    val df: DataFrame = sqlContext.createDataFrame(rowRDD, structType)
    df.createOrReplaceTempView("people")
    //使用SQL语句进行查询
    sqlContext.sql("select * from people").show()
  }
}
