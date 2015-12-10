import org.apache.spark.SparkContext

import org.apache.spark.sql.Row
//
import org.apache.spark.SparkConf

import org.apache.spark.sql.types._

import org.apache.spark.sql._


object myworksheet {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(241); 
  println("Welcome to the Scala worksheet");$skip(83); 
  
  val url="jdbc:mysql://192.168.27.235:3306/test?user=liujiyu&password=liujiyu";System.out.println("""url  : String = """ + $show(url ));$skip(86); 
//
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local");System.out.println("""conf  : org.apache.spark.SparkConf = """ + $show(conf ));$skip(34); 
    val sc=new SparkContext(conf);System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(59); 
    val sqlContext=new org.apache.spark.sql.SQLContext(sc);System.out.println("""sqlContext  : org.apache.spark.sql.SQLContext = """ + $show(sqlContext ));$skip(89); 
    
  
    val df=sqlContext.jsonFile("/user/liujiyu/spark/finalresultstatistics.json");System.out.println("""df  : org.apache.spark.sql.SchemaRDD = """ + $show(df ));$skip(67); 
    
		//df.filter(df("name")>21).show()
    
    df.printSchema()}
}
