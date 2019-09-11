package com.code.decre.spark.rdd.demo;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author liaofa
 * @date 2019/9/9
 */
public class SparkTransformationJava {
    static SparkConf conf = null;
    static JavaSparkContext sc = null;

    static {
        conf = new SparkConf();
        conf.setMaster("local").setAppName("TestTransformation");
        sc = new JavaSparkContext(conf);
    }

    public static void map() {
        String[] names = {"zs", "ls", "zw"};
        List<String> list = Arrays.asList(names);
        JavaRDD<String> listRDD = sc.parallelize(list);
        JavaRDD<String> nameRDD = listRDD.map(name -> {
            return "Hello " + name;
        });
        nameRDD.foreach(name -> System.out.println(name));
    }

    public static void flatMap() {
        String[] names = {"zs ls", "zw wl"};
        List<String> list = Arrays.asList(names);
        JavaRDD<String> listRDD = sc.parallelize(list);
        JavaRDD<String> nameRDD = listRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(name -> "Hello " + name);
        nameRDD.foreach(name -> System.out.println(name));
    }

    /**
     * map:
     * 一条数据一条数据的处理（文件系统，数据库等等）
     * mapPartitions：
     * 一次获取的是一个分区的数据（hdfs）
     * 正常情况下，mapPartitions 是一个高性能的算子
     * 因为每次处理的是一个分区的数据，减少了去获取数据的次数。
     * <p>
     * 但是如果我们的分区如果设置得不合理，有可能导致每个分区里面的数据量过大。
     */
    public static void mapPartitions() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        //参数二代表这个rdd里面有两个分区
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
        listRDD.mapPartitions(integerIterator -> {
            List<String> array = new ArrayList<>();
            while (integerIterator.hasNext()) {
                array.add("hello " + integerIterator.next());
            }
            return array.iterator();
        }).foreach(name -> System.out.println(name));

    }

    /**
     * 每次获取和处理的就是一个分区的数据,并且知道处理的分区的分区号是啥
     */
    public static void mapPartitionsWithIndex() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        //参数二代表这个rdd里面有两个分区
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
        listRDD.mapPartitionsWithIndex((index, iterator) -> {
            List<String> array = new ArrayList<>();
            while (iterator.hasNext()) {
                array.add(index + "_" + iterator.next());
            }
            return array.iterator();
        }, true).foreach(name -> System.out.println(name));
    }

    /**
     * 聚合操作
     */
    public static void reduce() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        //参数二代表这个rdd里面有两个分区
        JavaRDD<Integer> listRDD = sc.parallelize(list);

        Integer result = listRDD.reduce((x, y) -> x + y);
        System.out.println(result);
    }

    /**
     * reduceByKey仅将RDD中所有K,V对中K值相同的V进行合并
     */
    public static void reduceByKey() {
        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<>("wd", 99), new Tuple2<>("sl", 98), new Tuple2<>("wd", 96), new Tuple2<>("sl", 95));
        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> resultRDD = listRDD.reduceByKey((x, y) -> x + y);
        resultRDD.foreach(tuple -> System.out.println("mp: " + tuple._1 + " -> " + tuple._2));
    }

    /**
     * 合并rdd，必须保证两个RDD的泛型是一致的
     */
    public static void union() {
        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        final List<Integer> list2 = Arrays.asList(5, 6, 7, 8);
        final JavaRDD<Integer> listRDD1 = sc.parallelize(list1);
        final JavaRDD<Integer> listRDD2 = sc.parallelize(list2);
        listRDD1.union(listRDD2).foreach(num -> System.out.println(num));
    }

    /**
     * groupBy是将RDD中的元素进行分组，组名是call方法中的返回值，而顾名思义groupByKey是将PairRDD中拥有相同key值得元素归为一组
     */
    public static void groupBy() {
        List<Tuple2<String, String>> list = Arrays.asList(
                new Tuple2("武当", "张三丰"),
                new Tuple2("峨眉", "灭绝师太"),
                new Tuple2("武当", "宋青书"),
                new Tuple2("峨眉", "周芷若")
        );
        JavaPairRDD<String, String> listRDD = sc.parallelizePairs(list);

        JavaPairRDD<String, Iterable<String>> groupByKeyRDD = listRDD.groupByKey();
        groupByKeyRDD.foreach(tuple -> {
            String menpai = tuple._1;
            Iterator<String> iterator = tuple._2.iterator();
            String people = "";
            while (iterator.hasNext()) {
                people = people + iterator.next() + " ";
            }
            System.out.println("门派:" + menpai + "人员:" + people);
        });

    }

    /**
     * oin是将两个PairRDD合并，并将有相同key的元素分为一组，可以理解为groupByKey和Union的结合
     */
    public static void join() {
        final List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "林平之")
        );
        final List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 98),
                new Tuple2<Integer, Integer>(3, 97)
        );

        final JavaPairRDD<Integer, String> nemesrdd = sc.parallelizePairs(names);
        final JavaPairRDD<Integer, Integer> scoresrdd = sc.parallelizePairs(scores);

        final JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = nemesrdd.join(scoresrdd);
        joinRDD.foreach(tuple -> System.out.println("学号:" + tuple._1 + " 姓名:" + tuple._2._1 + " 成绩:" + tuple._2._2));
    }

    /**
     * 抽样
     */
    public static void sample() {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            list.add(i);
        }
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        /**
         * sample用来从RDD中抽取样本。他有三个参数
         * withReplacement: Boolean,
         *       true: 有放回的抽样
         *       false: 无放回抽象
         * fraction: Double：
         *      抽取样本的比例
         * seed: Long：
         *      随机种子
         */
        JavaRDD<Integer> sampleRDD = listRDD.sample(false, 0.1, 0);
        sampleRDD.foreach(num -> System.out.print(num + " "));
    }

    /**
     * 求笛卡尔积
     */
    public static void cartesian() {
        List<String> list1 = Arrays.asList("A", "B");
        List<Integer> list2 = Arrays.asList(1, 2, 3);
        JavaRDD<String> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.cartesian(list2RDD).foreach(tuple -> System.out.print(tuple._1 + "->" + tuple._2));
    }

    /**
     * 过滤出偶数
     */
    public static void filter() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> filterRDD = listRDD.filter(num -> num % 2 == 0);
        filterRDD.foreach(num -> System.out.print(num + " "));
    }

    /**
     * 去重
     */
    public static void distinct() {
        List<Integer> list = Arrays.asList(1, 1, 2, 2, 3, 3, 4, 5);
        JavaRDD<Integer> listRDD = (JavaRDD<Integer>) sc.parallelize(list);
        listRDD.distinct().foreach(num -> System.out.println(num));
    }

    /**
     * 交集
     */
    public static void intersection() {
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.intersection(list2RDD).foreach(num -> System.out.println(num));
    }

    /**
     * 分区数由多  -》 变少
     */
    public static void coalesce() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 3);
        listRDD.coalesce(1).foreach(num -> System.out.println(num));
    }

    /**
     * 进行重分区，解决的问题：本来分区数少  -》 增加分区数
     */
    public static void replication() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 1);
        listRDD.repartition(2).foreach(num -> System.out.println(num));
    }

    /**
     * repartitionAndSortWithinPartitions函数是repartition函数的变种，与repartition函数不同的是，repartitionAndSortWithinPartitions在给定的partitioner内部进行排序，性能比repartition要高。
     */
    public static void repartitionAndSortWithinPartitions(){
        List<Integer> list = Arrays.asList(1, 4, 55, 66, 33, 48, 23);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 1);
        JavaPairRDD<Integer, Integer> pairRDD = listRDD.mapToPair(num -> new Tuple2<>(num, num));
        pairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2))
                .mapPartitionsWithIndex((index,iterator) -> {
                    ArrayList<String> list1 = new ArrayList<>();
                    while (iterator.hasNext()){
                        list1.add(index+"_"+iterator.next());
                    }
                    return list1.iterator();
                },false)
                .foreach(str -> System.out.println(str));
    }

    /**
     * 对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
     */

    public static void cogroup(){
        List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<Integer, String>(1, "www"),
                new Tuple2<Integer, String>(2, "bbs")
        );

        List<Tuple2<Integer, String>> list2 = Arrays.asList(
                new Tuple2<Integer, String>(1, "cnblog"),
                new Tuple2<Integer, String>(2, "cnblog"),
                new Tuple2<Integer, String>(3, "very")
        );

        List<Tuple2<Integer, String>> list3 = Arrays.asList(
                new Tuple2<Integer, String>(1, "com"),
                new Tuple2<Integer, String>(2, "com"),
                new Tuple2<Integer, String>(3, "good")
        );

        JavaPairRDD<Integer, String> list1RDD = sc.parallelizePairs(list1);
        JavaPairRDD<Integer, String> list2RDD = sc.parallelizePairs(list2);
        JavaPairRDD<Integer, String> list3RDD = sc.parallelizePairs(list3);

        list1RDD.cogroup(list2RDD,list3RDD).foreach(tuple ->
                System.out.println(tuple._1+" " +tuple._2._1() +" "+tuple._2._2()+" "+tuple._2._3()));
    }

    /**
     * sortByKey函数作用于Key-Value形式的RDD，并对Key进行排序。它是在org.apache.spark.rdd.OrderedRDDFunctions中实现的，实现如下
     */
    public static void sortByKey(){
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<>(99, "张三丰"),
                new Tuple2<>(96, "东方不败"),
                new Tuple2<>(66, "林平之"),
                new Tuple2<>(98, "聂风")
        );
        JavaPairRDD<Integer, String> listRDD = sc.parallelizePairs(list);
        listRDD.sortByKey(false).foreach(tuple ->System.out.println(tuple._2+"->"+tuple._1));
    }

    /**
     * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，
     * 在聚合过程中同样使用了一个中立的初始值。和aggregate函数类似，
     * aggregateByKey返回值的类型不需要和RDD中value的类型一致。
     * 因为aggregateByKey是对相同Key中的值进行聚合操作，
     * 所以aggregateByKey函数最终返回的类型还是Pair RDD，
     * 对应的结果是Key和聚合好的值；而aggregate函数直接是返回非RDD的结果，这点需要注意。在实现过程中，定义了三个aggregateByKey函数原型，但最终调用的aggregateByKey函数都一致。
     */
    public static void aggregateByKey() {
        List<String> list = Arrays.asList("you,jump", "i,jump");
        JavaRDD<String> listRDD = sc.parallelize(list);
        listRDD.flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .aggregateByKey(0,(x,y)-> x+y,(m,n) -> m+n)
                .foreach(tuple -> System.out.println(tuple._1+"->"+tuple._2));
    }
}
