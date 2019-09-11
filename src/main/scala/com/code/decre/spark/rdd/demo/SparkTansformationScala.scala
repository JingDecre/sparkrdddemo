package com.code.decre.spark.rdd.demo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * @author liaofa
  * @date 2019/9/9
  */
object SparkTansformationScala {
  private val scalaconf: SparkConf = new SparkConf()
  scalaconf.setMaster("local")
  scalaconf.setAppName("TransformScala")
  private val sc: SparkContext = new SparkContext(scalaconf)

  def map() = {
    val list = List("zs", "ls", "zw")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.map(name => "Hello " + name)
    nameRDD.foreach(name => println(name))
  }

  def flatMap() = {
    val list = List("zs ls", "zw wl")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.flatMap(line => line.split(" ")).map(name => "Hello " + name)
    nameRDD.foreach(name => println(name))
  }

  /**
    * map:
    * 一条数据一条数据的处理（文件系统，数据库等等）
    * mapPartitions：
    * 一次获取的是一个分区的数据（hdfs）
    * 正常情况下，mapPartitions 是一个高性能的算子
    * 因为每次处理的是一个分区的数据，减少了去获取数据的次数。
    *
    * 但是如果我们的分区如果设置得不合理，有可能导致每个分区里面的数据量过大。
    */
  def mapParations(): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list)

    listRDD.mapPartitions(iterator => {
      val newList: ListBuffer[String] = ListBuffer()
      while (iterator.hasNext) {
        newList.append("hello " + iterator.next())
      }
      newList.toIterator
    }).foreach(name => println(name))
  }

  /**
    * 每次获取和处理的就是一个分区的数据,并且知道处理的分区的分区号是啥
    */
  def mapParationsWithIndex(): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list)
    listRDD.mapPartitionsWithIndex((index, iterator) => {
      val newList: ListBuffer[String] = ListBuffer()
      while (iterator.hasNext) {
        newList.append(index + "_" + iterator.next())
      }
      newList.iterator
    }).foreach(name => println(name))

  }

  def reduce(): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list)
    val result = listRDD.reduce((x, y) => x + y)
    println(result)
  }

  /**
    * reduceByKey仅将RDD中所有K,V对中K值相同的V进行合并
    */
  def reduceByKey(): Unit = {
    val list = List(("wd", 99), ("sl", 97), ("wd", 95), ("sl", 96))
    val listRDD = sc.parallelize(list)
    val resultRDD = listRDD.reduceByKey(_ + _)
    resultRDD.foreach(tuple => println("mp: " + tuple._1 + " -> " + tuple._2))
  }

  /**
    * 合并
    */
  def union(): Unit = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(5, 6, 7, 8)
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)
    rdd1.union(rdd2).foreach(num => println(num))
  }

  def groupByKey(): Unit = {
    val list = List(("武当", "张三丰"), ("峨眉", "灭绝师太"), ("武当", "宋青书"), ("峨眉", "周芷若"))
    val listRDD = sc.parallelize(list)
    val groupByKeyRDD = listRDD.groupByKey()
    groupByKeyRDD.foreach(t => {
      val menpai = t._1
      val iterator = t._2.iterator
      var people = ""
      while (iterator.hasNext) people = people + iterator.next + " "
      println("门派:" + menpai + "人员:" + people)
    })
  }

  def join(): Unit = {
    val list1 = List((1, "东方不败"), (2, "令狐冲"), (3, "林平之"))
    val list2 = List((1, 99), (2, 98), (3, 97))
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)

    val joinRDD = list1RDD.join(list2RDD)
    joinRDD.foreach(t => println("学号:" + t._1 + " 姓名:" + t._2._1 + " 成绩:" + t._2._2))

  }

  def sample(): Unit = {
    val list = 1 to 100
    val listRDD = sc.parallelize(list)
    listRDD.sample(false, 0.1, 0).foreach(num => print(num + " "))
  }

  def cartesian(): Unit = {
    val list1 = List("A", "B")
    val list2 = List(1, 2, 3)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    list1RDD.cartesian(list2RDD).foreach(t => println(t._1 + "->" + t._2))
  }

  def filter(): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val listRDD = sc.parallelize(list)
    listRDD.filter(num => num % 2 == 0).foreach(print(_))
  }

  def distinct(): Unit = {
    val list = List(1, 1, 2, 2, 3, 3, 4, 5)
    sc.parallelize(list).distinct().foreach(println(_))
  }

  def intersection(): Unit = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    list1RDD.intersection(list2RDD).foreach(println(_))
  }

  def coalesce(): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    sc.parallelize(list, 3).coalesce(1).foreach(println(_))
  }

  def replication(): Unit = {
    val list = List(1, 2, 3, 4)
    val listRDD = sc.parallelize(list, 1)
    listRDD.repartition(2).foreach(println(_))
  }

  def repartitionAndSortWithinPartitions(): Unit ={
    val list = List(1, 4, 55, 66, 33, 48, 23)
    val listRDD = sc.parallelize(list,1)
    listRDD.map(num => (num,num))
      .repartitionAndSortWithinPartitions(new HashPartitioner(2))
      .mapPartitionsWithIndex((index,iterator) => {
        val listBuffer: ListBuffer[String] = new ListBuffer
        while (iterator.hasNext) {
          listBuffer.append(index + "_" + iterator.next())
        }
        listBuffer.iterator
      },false)
      .foreach(println(_))

  }

  def cogroup(): Unit ={
    val list1 = List((1, "www"), (2, "bbs"))
    val list2 = List((1, "cnblog"), (2, "cnblog"), (3, "very"))
    val list3 = List((1, "com"), (2, "com"), (3, "good"))

    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    val list3RDD = sc.parallelize(list3)

    list1RDD.cogroup(list2RDD,list3RDD).foreach(tuple =>
      println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2 + " " + tuple._2._3))
  }

  def sortByKey(): Unit ={
    val list = List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风"))
    sc.parallelize(list).sortByKey(false).foreach(tuple => println(tuple._2 + "->" + tuple._1))
  }

  def aggregateByKey(): Unit ={
    val list = List("you,jump", "i,jump")
    sc.parallelize(list)
      .flatMap(_.split(","))
      .map((_, 1))
      .aggregateByKey(0)(_+_,_+_)
      .foreach(tuple =>println(tuple._1+"->"+tuple._2))
  }


}
