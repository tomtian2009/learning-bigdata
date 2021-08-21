package com.tom.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AdStat {
  def main(args: Array[String]): Unit = {

    val inputFile =  "datas/agent.log"
    val sparkConf = new SparkConf().setMaster("local").setAppName("AdStat")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 编写广告统计
    //时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    //统计出每一个省份每个广告被点击数量排行的 Top3
    //省份，广告，次数   递减排序
    val fileRDD: RDD[String] = sc.textFile(inputFile, 2)
    val mapRDD: RDD[((String, String), Int)] = fileRDD.map(line => {
      val words: Array[String] = line.split(" ")
      //((省份，广告),1)
      ((words(1), words(4)), 1)
    })
    //((省份，广告),次数)
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey((x, y) => x + y)
    val prvAdcnt: RDD[(String, (String, Int))] = reduceRDD.map {
          //（省份，（广告，次数））
      case ((prv, ad), cnt) => (prv, (ad, cnt))
    }

    //按照省份进行分组 （省份，[(广告1，次数1),(广告2，次数2)...]）
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvAdcnt.groupByKey()
    groupRDD.mapValues( item => {
      item.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    }).collect().foreach(println)


    sc.stop()

  }
}
