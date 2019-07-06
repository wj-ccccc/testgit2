//综合性的例子
//求f(x)的和，x就是a到b的值
def sum(f:Int=>Int)(a:Int)(b:Int)={
  @annotation.tailrec
  def loop(n:Int,acc:Int): Unit ={
    if(n>b){
      println(s"n:${n}  acc:${acc}")
      acc
    }else{
      println(s"n:${n}  acc:${acc}")
      loop(n+1,f(n)+acc)
    }
  }
  loop(a,0)
}

sum(x=>x)(1)(5)//1+2+3+4+5

sum(x=>x*x)(1)(5)//1*1+2*2+3*3+4*4+5*5

val sc=sum(x=>x*x*x)_
sc(1)(5)


def walta(l:List[Int]): Unit ={
  if(l.isEmpty==false){
   // l.head.toString+"sss"+walta(l.tail)
    println(l.head.toString)
    walta(l.tail)
  }

}
val li=List(1,2,3,4,5)
walta(li)

val scli=0::li

val op=scli:::li






