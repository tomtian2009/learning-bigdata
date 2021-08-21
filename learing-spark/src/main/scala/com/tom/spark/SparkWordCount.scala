package com.tom.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SparkWordCount {
  def main(args: Array[String]) {
    val inputFile =  "datas/word.txt"
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)
    val fileRDD: RDD[String] = sc.textFile(inputFile)
//    val word2Count: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val word2Count: RDD[(String, Int)] = fileRDD.flatMap(line => line.split(" "))
      .map(word => (word, 1)).reduceByKey((x, y) => x + y)
    word2Count.collect().foreach(println)
    sc.stop()
  }
}
