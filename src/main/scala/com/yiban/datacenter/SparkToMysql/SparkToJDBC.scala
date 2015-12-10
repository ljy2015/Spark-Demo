package com.yiban.datacenter.SparkToMysql

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf

import org.apache.spark.sql._
//

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }

class SparkToJDBC {

}

case class Person(name: String, age: Int)

object SparkToJDBC {
  def main(args: Array[String]): Unit = {

    /**
     * Spark -SQL simple query
     */

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

   // import sqlContext.implicits._

    /**
     * //by Case class inferring  the Schema
     */
    /*
    val people = sc.textFile("/user/liujiyu/spark/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()

    people.registerTempTable("people")
    
    val teenagerss = sqlContext.sql("SELECT name,age FROM people WHERE age >= 13 AND age <= 30")
    
    teenagerss.map(t => {"Name: " + t(0) +"age: "+t(1)}).collect().foreach(println)*/

    /**
     * //by Programmatically specifying the Schema
     */

    val people = sc.textFile("/user/liujiyu/spark/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Import Spark SQL data types and Row.
    import org.apache.spark.sql._

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    results.map(t => "Name: " + t(0)).collect().foreach(println) 

    /**
     * Data Source
     */
    import sqlContext.implicits._

    //convert DFRDD to Parquet File 
    peopleDataFrame.saveAsParquetFile("people.parquet"); //people.parquet save on current user's home
    val parquetFile = sqlContext.parquetFile("people.parquet")
    parquetFile.registerTempTable("parquetFile")
    val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 30")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    /**
     * Schema merging
     */

    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.saveAsParquetFile("data/test_table/key=1")

    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.saveAsParquetFile("data/test_table/key=2")

    val df3 = sqlContext.parquetFile("data/test_table")
    df3.printSchema() 

    /**
     * JSON Datasets
     */

    // sc is an existing SparkContext.
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val path = "/user/liujiyu/myjson.json"
    // Create a DataFrame from the file(s) pointed to by path
    val peoples = sqlContext.jsonFile(path)

    // The inferred schema can be visualized using the printSchema() method.
    peoples.printSchema()
    // root
    //  |-- age: integer (nullable = true)
    //  |-- name: string (nullable = true)

    // Register this DataFrame as a table.
    peoples.registerTempTable("peoples")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers1 = sqlContext.sql("SELECT name FROM peoples WHERE age >= 13 AND age <= 30")
    
    teenagers1.map(t=>("Name: "+t(0))).collect().foreach(println)
    
    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
    anotherPeople.registerTempTable("anotherPeoples")
    
    val teenagers2 = sqlContext.sql("SELECT * FROM anotherPeoples ")

    teenagers2.map(t=>("Name: "+t(0)+ "lenght: "+t.length +"second: "+t(1))).collect().foreach(println)
    
    
 
   
    sc.stop
  }

}