package com.code.decre.spark.rdd.demo

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author liaofa
  * @date 2019/9/11
  *      通过 json 文件创建 DataFrames
  */
object TestDataFrame3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDataFrame2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df: DataFrame = sqlContext.read.json("E:\\666\\people.json")
    df.createOrReplaceTempView("people")
    sqlContext.sql("select * from people").show()
  }
}
