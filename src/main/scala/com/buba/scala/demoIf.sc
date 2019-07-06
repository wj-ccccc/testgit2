def curriedAdd(a:Int)(b:Int)=a+b

val addOne=curriedAdd(1)_
 addOne(1)


@annotation.tailrec  //annotation.tailrec 告诉scala编译器进行尾递归优化
def fanctorial(i:Int,n:Int):Int=
  if(i==0)n
  else fanctorial(i - 1,n*i)


fanctorial(3,3)





val a = List("num1", "num22", "num333")

val s = 1
if (s == 1) s"123"

if (s != 1) s"123"


for (
  s <- a
) println(s)

for {
  s <- a
  if (s.length > 4)
} println(s)

var result_for = for {
  s <- a
  s1 = s.toUpperCase()
  if (s1 != "")
} yield (s1)

val sc = try {
  Integer.parseInt("a")
} catch {
  case _ => 0
} finally {
  println("aaaaa")
}

val q = 34
val sss=q match {
  case 1 => "one"
  case 2 => "2one"
  case 3 => "3one"
  case _ =>"def"
}

def greeting()=(name:String)=>s"hello,"+name
greeting()












