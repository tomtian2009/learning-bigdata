package com.tom.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkInvertedIndex {

  def main(args: Array[String]) {
    val inputFile = "datas/index"
    val sparkConf = new SparkConf().setMaster("local").setAppName("SparkInvertedIndex")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 读取文件内容，创建 RDD 对象
    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles(inputFile)

    //(文件名，文件内容)  文件内容中的换行符已替换为空格
    val mapRDD: RDD[(String, String)] = inputRDD.map {
      case (filePath, fileContent) => {
        val fileName: String = filePath.split("/").last
        val content: String = fileContent.replaceAll(System.lineSeparator(), " ")
        (fileName, content)
      }
    }

    //(文件名，单词)
    val fileNameWordRDD: RDD[(String, String)] = mapRDD.flatMapValues(_.split(" ")).cache()

    // 反向文件索引，并在控制台输出结果
    calcInvertedIndex(fileNameWordRDD)

    // 反向文件索引带上词频，并在控制台输出结果
    calcInvertedIndexWithWordFreq(fileNameWordRDD)

    sc.stop()


  }

  def calcInvertedIndex(fileWordRDD : RDD[(String, String)]) : Unit = {
    //(单词，文件名)
    val wordFileName: RDD[(String, String)] = fileWordRDD.map {
      case (fileName, word) => (word, fileName)
    }

    //(单词，[（文件1），（文件2），（文件3）...])，并按单词排序
    val invertedIndex: RDD[(String, Iterable[String])] = wordFileName.groupByKey().sortByKey()

    // (单词，[（单词出现的文件名1），（文件2），（文件3）...])，文件名已去重
    val distinctInvertedIndex: RDD[(String, List[String])] = invertedIndex.mapValues(iterator => iterator.toList.distinct)

    println("*****************反向文件索引统计：*****************")
    distinctInvertedIndex.collect().foreach(
      item => println(item._1+" : "+item._2.mkString("{",",","}"))
    )

  }


  def calcInvertedIndexWithWordFreq(fileWordRDD : RDD[(String, String)]) : Unit = {
    //（(单词，文件名)，出现次数）=> (单词，（文件名，出现次数）)
    val wordFileCounts: RDD[(String, (String, Int))] = fileWordRDD.map {
      case (fileName, word) => ((word, fileName), 1)
    }.reduceByKey(_ + _).map {
      case ((word, fileName), count) => (word, (fileName, count))
    }

    //(单词，[（文件1，出现次数），（文件2，出现次数），（文件3，出现次数）...])，并按单词排序
    val invertedIndex: RDD[(String, Iterable[(String, Int)])] = wordFileCounts.groupByKey().sortByKey()

    // (单词，List[（文件1，出现次数），（文件2，出现次数），（文件3，出现次数）...]) ,按 valuse 中的文件名排序
    val sortedInvertedIndex: RDD[(String, List[(String, Int)])] = invertedIndex.mapValues(item => {
      item.toList.sortBy(_._1)(Ordering.String)
    })
    println("*****************带词频的反向文件索引统计：*****************")
    sortedInvertedIndex.collect().foreach(
      item => println(item._1+" : "+item._2.mkString("{",",","}"))
    )

  }

}
