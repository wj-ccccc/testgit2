package com.buba.scala

/**
  * ━━━━━━神兽出没━━━━━━
  * 　　　┏┓　　　┏┓
  * 　　┏┛┻━━━┛┻┓
  * 　　┃　　　　　　　┃
  * 　　┃　　　━　　　┃
  * 　　┃　┳┛　┗┳　┃
  * 　　┃　　　　　　　┃
  * 　　┃　　　┻　　　┃
  * 　　┃　　　　　　　┃
  * 　　┗━┓　　　┏━┛
  * 　　　　┃　　　┃神兽保佑, 永无BUG!
  * 　　　　┃　　　┃Code is far away from bug with the animal protecting
  * 　　　　┃　　　┗━━━┓
  * 　　　　┃　　　　　　　┣┓
  * 　　　　┃　　　　　　　┏┛
  * 　　　　┗┓┓┏━┳┓┏┛
  * 　　　　　┃┫┫　┃┫┫
  * 　　　　　┗┻┛　┗┻┛
  * ━━━━━━感觉萌萌哒━━━━━━
  * Module Desc:
  * User: wujian
  * DateTime: 2019/7/1 15:06
  */
object ErFen {
  /*def main(args: Array[String]): Unit = {
    val list = List(3, 12, 43, 23, 7, 1, 2, 0)
    println(sort(list))
  }
  //定义一个方法，传入的参数是要进行排序的List集合，输出的是排序后的List集合
  def sort(list: List[Int]): List[Int] = list match {
    case List() => List()
    case head :: tail => compute(head, sort(tail))
  }
  def compute(data: Int, dataSet: List[Int]): List[Int] = dataSet match {
    case List() => List(data)
    case head :: tail => if (data <= head) data :: dataSet else * head :: compute(data, tail)
  }*/
  def sort(list: List[Int]): List[Int] = list match {
    case List() => List()
    case head :: tail => compute(head, sort(tail))
  }

  def compute(data: Int, dataSet: List[Int]): List[Int] = dataSet match {
    case List() => List(data)
    case head :: tail => if (data <= head) data :: dataSet else head :: compute(data, tail)
  }

  def main(args: Array[String]) {
    val list = List(3, 12, 43, 23, 7, 1, 2, 0)
    println(quickSort(list))
  }

  def quickSort(list: List[Int]): List[Int] = {
    //对输入参数list进行模式匹配
    list match {
      //如果是空，返回nil
      case Nil => Nil
      case List() => List()
      //不为空从list中提取出首元素和剩余元素组成的列表分别到head和tail中
      case head :: tail =>
        //对剩余元素列表调用partition方法，这个方法会将列表分为两部分。
        // 划分依据接受的参数，这个参数是一个函数(这里是(_ < x))。
        // partition方法会对每个元素调用这个函数，根据返回的true,false分成两部分。
        // 这里’_ < x’是一个匿名函数(又称lambda),’_’关键字是函数输入参数的占位符，
        // 输入参数这里是列表中的每个元素。
        val (left, right) = tail.partition(_ < head)
        println(left,right)
        //最后对划分好的两部分递归调用quickSort
        //其中head::quickSort(right) 这里::是List定义的一个方法，用于将两部分合成一个列表
        quickSort(left) ++ (head :: quickSort(right))
    }
  }
}
