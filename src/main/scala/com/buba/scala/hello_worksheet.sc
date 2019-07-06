object hello {
  /*
  * Scala的函数可以足够聪明知道函数的返回值类型

    Scala的函数简单的一行表达式不需要函数括号

  * */
  def helloa(name: String): String = {
    s"hello, ${name}"
  }

  helloa("武剑")

  def hello2(name: String) = {
   s"hello, ${name}"
  }

  hello2("武剑")

  def add(x: Int, y: Int) = x + y
  add(1,2)


//list高阶函数：
  val listnum = List(1,2,3,4,5,6,7,8,9)
  listnum.filter(x =>x%2==0)
  "99 red boolean".toList.filter(x=>Character.isDigit(x))
  "99 red boolean".toList.takeWhile(x=>x!='b')
//根据函数规则对List中的每个参数做映射
  val a = List("num1", "num22", "num333")
  a.map(_.toUpperCase)
  listnum.filter(_%2==0).map(_+10)

  val q=List(listnum,List(12,13,14,15,16,17))

  q.map((_,1))

  q.flatMap(_.filter(_%2==0))
//List规约操作：
  listnum.reduceLeft((x,y)=>x+y)
  listnum.reduceLeft(_*_)
  listnum.foldLeft(1)(_*_)

//range
  (1 to 10 by 2).toList//产生一个range，步长为2
  (1 until  10).toList//不包含尾
//Stream：惰性List和lazy一样
  val stra=1 #:: 2 #:: 3 #:: Stream.empty
  val str2=(1985 to 100000000).toStream
  str2.head
  str2.tail

  //Tuple元组
  /*
  * 如果元组只有2个值，成为一个pari，一个对
    定义一个元组t,取第一个元素的方式是t._1
    元组的应用：比如说一个函数一般只返回一个值，使用元组封装一下可以包含多个值
  * */
  val sdf=(1,2,3,4)
  sdf._1
  sdf._2
  sdf._3

    //求集合的个数，和，乘积，返回一个tuple
  def sumSF(l:List[Int]):(Int,Int,Int)={
    l.foldLeft(0,0,0)((x,v)=>(x._1+1,x._2+v,v*v))
  }
  sumSF(listnum)

  val smap=Map(1->"asd",22->"gd",3->"?")
  smap(1)
  smap + (5->12)
  smap ++ List(11->"a",23->"l")
  val vmap=Map("q"->1,"as"->2,"q"->4)//重复直接替换

  smap.keys
  smap.values
  /*println("hello wrold")
  var x = 2
  println(x)*/
}
