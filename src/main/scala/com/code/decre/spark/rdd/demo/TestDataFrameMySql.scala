package com.code.decre.spark.rdd.demo

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author liaofa
  * @date 2019/9/11
  */
object TestDataFrameMySql {
  val conf = new SparkConf().setAppName("TestMysql").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val url = "jdbc:mysql://192.168.123.102:3306/hivedb"
  val table = "dbs"
  val properties = new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","root")
  //需要传入Mysql的URL、表明、properties（连接数据库的用户名密码）
  val df = sqlContext.read.jdbc(url,table,properties)
  df.createOrReplaceTempView("dbs")
  sqlContext.sql("select * from dbs").show()
}
