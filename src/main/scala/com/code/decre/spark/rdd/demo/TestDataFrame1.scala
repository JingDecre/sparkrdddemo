package com.code.decre.spark.rdd.demo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author liaofa
  * @date 2019/9/11
  */
//定义case class，相当于表结构
case class People(var name:String, var age:Int)
object TestDataFrame1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)
    // 将本地的数据读入 RDD， 并将 RDD 与 case class 关联
    val peopleRDD = sc.textFile("D:\\dragonsoft\\test-txt\\people.txt").map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))
    import context.implicits._
    // 将RDD 转换成 DataFrames
    val df = peopleRDD.toDF
    //将DataFrames创建成一个临时的视图
    df.createOrReplaceTempView("people")
    //使用SQL语句进行查询
    context.sql("select * from people").show()
  }
}
