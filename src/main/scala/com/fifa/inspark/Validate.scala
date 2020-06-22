package com.fifa.inspark

import org.apache.spark.sql.DataFrame

import scala.collection.immutable.ListMap
import scala.util.control.Breaks

object Validate {
  def getAttributes(df:DataFrame, num:Int): String ={
    var columnMap = scala.collection.mutable.Map[String, Double]()
    var x = 0
    var attribute = ""
    val avg_Values = df.collectAsList().get(0)
    for(itr <- df.columns){
      columnMap(itr) = avg_Values.get(x).toString.toDouble
      x=x+1
    }
    val sortedMap = ListMap(columnMap.toSeq.sortWith(_._2 > _._2):_*)
    x=0
    val loop = new Breaks;
    loop.breakable {
      for (out <- sortedMap) {
        attribute += out._1.replace("_avg(","")
        x += 1
        if (x == num) {
          attribute += "."
          loop.break
        }else{
          attribute += "\t  "
        }
      }
    }
    attribute
  }

}
