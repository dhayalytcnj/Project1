package com.revature.spark

import org.apache.spark.sql._
import scala.io.StdIn._

object Project1 {

//----------Scenarios

    def scenario1(spark: SparkSession): Unit={
        println("\nScenario 1:");
        println("Total number of consumers for Branch 1: ")
        spark.sql("select sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch1'").show();
        println("\nTotal number of consumers for Branch 2: ")
        spark.sql("select sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch2'").show();
    }


    def scenario2(spark: SparkSession): Unit={
        println("\nScenario 2:");
        println("Most consumed beverage of Branch 1:")
        spark.sql("select branches.drink, sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch1' group by branches.drink order by sum(counts.count) desc limit 1").show();
        println("Most consumed beverage of Branch 2:")
        spark.sql("select branches.drink, sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch2' group by branches.drink order by sum(counts.count) asc limit 1").show();
        println("Average of Branch 2 consumed beverages:")
        spark.sql("select round(avg(total), 0) from (Select branches.drink, sum(counts.count) as total from branches join counts on branches.drink = counts.drink where branches.branch = 'Branch2' group by branches.drink) as sumbranch2").show();
    }


    def scenario3(spark: SparkSession): Unit={
        println("\nScenario 3:")
        println("Beverages available on Branch10, Branch8, and Branch1:");
        spark.sql("select distinct drink from branches where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1'").show();
        println("Common beverages available in Branch4 and Branch7:");
        spark.sql("select distinct drink from branches where branch = 'Branch4' intersect select distinct drink from branches where branch = 'Branch7'").show();
    }


    def scenario4(spark: SparkSession): Unit={
        //Partition
        println("Partition of Scenario 3, part 1:");
        spark.sql("drop table if exists scenario4_t");
        print("creating table similar to branches...")
        spark.sql("create table scenario4_t like branches");
        spark.sql("insert into scenario4_t select * from branches")
        print("adding partition property to copied table...")
        spark.sql("alter table scenario4_t add partition (branch String)")
        print("Displaying Scenario 3 part 1 query on partitioned table...")
        spark.sql("select distinct drink from branches where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1'");
        //View
        println("View of Scenario 3, part 2:");
        spark.sql("drop view if exists scenario4_v")
        print("Creating view of Scenario 3 part 2...")
        spark.sql("create view scenario4_v as (select distinct drink from branches where branch = 'Branch4' intersect select distinct drink from branches where branch = 'Branch7')");
        print("Displaying view...")
        spark.sql("select * from scenario4_v").show();
    }


    def scenario5(spark: SparkSession): Unit={

    }


    def scenario6(spark: SparkSession): Unit={

    }

//----------Main Statement
    def main(args: Array[String]): Unit = {
        // create a spark session
        // for Windows
        System.setProperty("hadoop.home.dir", "C:\\winutils")

        val spark = SparkSession
          .builder()
          .appName("HiveTest5")
          .config("spark.master", "local")
          .enableHiveSupport()
          .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        /*
        spark.sql("create table if not exists branches(drink String,branch String) row format delimited fields terminated by ','");
        spark.sql("create table if not exists counts(drink String,count Int) row format delimited fields terminated by ','");
        spark.sql("load data local inpath 'input/Bev_BranchA.txt' into table branches");
        spark.sql("load data local inpath 'input/Bev_BranchB.txt' into table branches");
        spark.sql("load data local inpath 'input/Bev_BranchC.txt' into table branches");

        spark.sql("load data local inpath 'input/Bev_ConscountA.txt' into table counts");
        spark.sql("load data local inpath 'input/Bev_ConscountB.txt' into table counts");
        spark.sql("load data local inpath 'input/Bev_ConscountC.txt' into table counts");
*/
        println("---------------------------------------------");
        println(s"\nWelcome to Yash's Proje1ct 1");
        println("");

        var menu_option:Int = 0;
        while (menu_option != 7) {
            println("\n- Select an option:\n" + "1. Scenario 1\n" + "2. Scenario 2\n" +
              "3. Scenario 3\n" + "4. Scenario 4\n" + "5. Scenario 5\n" + "6. Scenario 6\n" + "7. Exit");

            print("\nOption: ")
            menu_option = readInt();

            menu_option match {
                case 1 => scenario1(spark);
                case 2 => scenario2(spark);
                case 3 => scenario3(spark);
                case 4 => scenario4(spark);
                case 5 => scenario5(spark);
                case 6 => scenario6(spark);
                case 7 => println("Goodbye!\n");
                case _ => println("please enter valid input\n");
            }
        }
        spark.close();
        System.exit(0);
    }
}
