package com.tom.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("AdStat")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val ints: Array[Int] = rdd.collect()
//    ints.toList.foreach(println)
    println(ints.mkString(","))

    sc.stop()
  }

}
