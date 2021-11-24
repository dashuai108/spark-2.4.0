
package org.apache.spark.sql.mytest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
object Test {
  def modifySourceCode(): Unit = {
    val sparkConf = new SparkConf()

    sparkConf.setMaster("local")

    sparkConf.setAppName("dashuai_local")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val read = sqlContext.read
    read.format("csv")
    read.option("encoding", "utf-8")
    read.option("header", true)
    read.option("multiLine", "false")
    read.option("mode", "PERMISSIVE")
    read.option("sep", ",")
//    read.option("timestampFormats", "yyyy=mm=dd,yyyy+MM+dd,yyyy%MM%dd,yyyy/MM/dd")
    read.option("timestampFormats", "yyyy%MM%dd,yyyy/MM/dd")
//    read.option("timestampFormat", "yyyy=MM=dd")
    read.option("inferSchema", true)
//    read.option("dateFormat", "dd-MM-yyyy hh:mm")

//  1223568000000000

    val df = read.load("d:/mnt/file/data/aps10.csv")
    //  yyyy=mm=dd
    println(df.describe())

    println(df.schema)

    println(df.show(3))

  }

  def modifyParameters(): Unit = {
    val sparkConf = new SparkConf()

    sparkConf.setMaster("local")

    sparkConf.setAppName("dashuai_local")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val read = sqlContext.read
    read.format("csv")
    read.option("encoding", "utf-8")
    read.option("header", true)
    read.option("multiLine", "false")
    read.option("mode", "PERMISSIVE")
    read.option("sep", ",")
    read.option("timestampFormat", "yyyy+MM+dd")
    read.option("inferSchema", true)
//        read.option("dateFormat", "dd-MM-yyyy hh:mm")

    val df = read.load("d:/mnt/file/data/aps10.csv")

    println(df.describe())

    println(df.schema)
    println(df.show(5))
  }

  def main(args: Array[String]): Unit = {
    modifySourceCode()
//    modifyParameters()
  }

}
