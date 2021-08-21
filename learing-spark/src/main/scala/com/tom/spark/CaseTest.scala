package com.tom.spark

object CaseTest {
  def main(args: Array[String]): Unit = {
    val res: List[String] = List(1, 2, 3).map {
      case 1 => "first"
      case 2 => "second"
      case _ => "other"
    }
    println("第一个输出结果："+res)

    val res2: List[String] = List(1, 2, 3, 4).map( item => {
      item match {
        case 1 => "first"
        case 2 => "second"
        case x:Int => "three"
        case _ => "other"
      }
    }
    )
    println("第2个输出结果："+res2)

  }

}
