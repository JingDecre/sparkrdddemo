package com.code.decre.spark.rdd.demo

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author liaofa
  * @date 2019/9/11
  */
object TestDataFrameSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDataFrame2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df1 = sqlContext.read.json("E:\\666\\people.json")
    //方式一
    df1.write.json("E:\\111")
    df1.write.parquet("E:\\222")
    //方式二
    df1.write.format("json").save("E:\\333")
    df1.write.format("parquet").save("E:\\444")
    //方式三
    df1.write.save("E:\\555")
    // 设置保存模式
    df1.write.format("parquet").mode(SaveMode.Ignore).save("E:\\444")

  }
}
