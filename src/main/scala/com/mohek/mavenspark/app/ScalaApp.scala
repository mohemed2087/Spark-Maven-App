package com.mohek.mavenspark.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ScalaApp{

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("Spark-Maven-App").setMaster("local")
    val sc:SparkContext = new SparkContext(conf)

    val sample = Array(("Fred", 88), ("Fred", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
    val sampleRdd = sc.parallelize(sample)

    val combiner = (x : Int,y : Int) => if (x>y) x else y
    val merger = (x : Int,y : Int) => if (x>y) x else y

    val maxRdd = sampleRdd.aggregateByKey(0)(combiner,merger)

    maxRdd.collect.foreach(println)
  }

}
