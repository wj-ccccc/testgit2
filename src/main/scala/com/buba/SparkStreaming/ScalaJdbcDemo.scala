package com.buba.scala

import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * Create by Wj on 2019/6/16 20:21
  */
object ScalaJdbcDemo {
  def main(args: Array[String]): Unit = {
    /*
    * setup()：加载默认配置db.default.*
    * setup('ch)：加载自定义配置db.ch.*
    * */
    DBs.setup()
    // 查询
    val result: List[(Int, String, String, Integer)] = DB.readOnly { implicit se =>
      SQL("select * from xiaoshuo").map(res => {
        //获取第一列的值  从1开始,或者传入列名
        val id = res.int(1)
        val xsm = res.string("xsm")
        val zz = res.string(3)
        //或者通过get获取
        val zdj = res.get[Integer]("zdj")
        (id, xsm, zz, zdj)
      }).list().apply()
    }
    // result.foreach(println)
    //根据条件查询
    val result2 = DB.readOnly { implicit se =>
      SQL("select * from xiaoshuo where id=?").bind("559683").map(res => {
        //获取第一列的值  从1开始,或者传入列名
        (res.string("xsm"))
        // (res.string(2))
      }).list().apply()
    }
    // result2.foreach(println)
    //添加删除
    DB.autoCommit { implicit se =>
      SQL("delete  from xiaoshuo where id= ?").bind(1213123).update().apply()
      // SQL("insert into xxx values(?,?,?)").bind(1,2,3).update().apply()
    }
    //事务
    DB.localTx { implicit se =>
      SQL("insert into p values(?,?)").bind(1, 2).update().apply()
      2/0
      SQL("insert into p values(?,?)").bind(1, 9).update().apply()
    }
  }
}
