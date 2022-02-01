package com.revature.spark

import org.apache.spark.{SparkConf, SparkContext}

object Project1 {

    def main(args: Array[String]) = {
        val conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Project1");

        val sc = new SparkContext(conf)

        //create RDD

        val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6));
        rdd1.collect().foreach(println);
    }
}
