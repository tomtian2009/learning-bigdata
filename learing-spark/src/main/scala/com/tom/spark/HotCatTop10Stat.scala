package com.tom.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCatTop10Stat {

  def main(args: Array[String]): Unit = {

    val inputFile =  "datas/user_visit_action.txt"
    val sparkConf = new SparkConf().setMaster("local").setAppName("HotCatTop10Stat")
    val sc: SparkContext = new SparkContext(sparkConf)

    val fileRDD: RDD[String] = sc.textFile(inputFile, 2)

    //(品类ID, (1,0,0)
    //(品类ID, (0,1,0)
    //(品类ID, (0,0,1)
    val flatRDD: RDD[(String, (Int, Int, Int))] = fileRDD.flatMap(line => {
      val datas: Array[String] = line.split("_")
      if (datas(6) != "-1") {
        Array((datas(6), (1, 0, 0)))
      } else if (datas(8) != "null") {
        val ids: Array[String] = datas(8).split(",")
        ids.map(catId => (catId, (0, 1, 0)))
      } else if (datas(10) != "null") {
        val ids: Array[String] = datas(10).split(",")
        ids.map(catId => (catId, (0, 0, 1)))
      } else {
        Nil
      }

    })
    //(品类ID, (点击数量,下单数量,支付数量)
    val hotCatRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    val resultRDD: Array[(String, (Int, Int, Int))] = hotCatRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)





    sc.stop()
  }
}
