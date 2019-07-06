//快速排序 递归
def qShort(li:List[Int]):List[Int]={
  if(li.length<2){
    li
  }else{
    qShort(li.filter(li.head>_))++ //判断比第一个元素大的放到右边
    li.filter(_==li.head)++ //加上中间的元素
    qShort(li.filter(li.head<_))//判断比第一个元素小的放到左边
  }
}
// 也就是说把数组 查分2半  左边的要小于右边的
qShort(List(49,38,65,97,76,13,27,49,78,34,12,64,5,4,62,99,98,54,56,17,18,23,34,15,35,25,53,51,43))









