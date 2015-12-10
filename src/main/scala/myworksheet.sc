import org.apache.spark.SparkContext

import org.apache.spark.sql.Row
//
import org.apache.spark.SparkConf

import org.apache.spark.sql.types._

import org.apache.spark.sql._


object myworksheet {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val url="jdbc:mysql://192.168.27.235:3306/test?user=liujiyu&password=liujiyu"
                                                  //> url  : String = jdbc:mysql://192.168.27.235:3306/test?user=liujiyu&password=
                                                  //| liujiyu
//
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
                                                  //> conf  : org.apache.spark.SparkConf = org.apache.spark.SparkConf@d8f459
    val sc=new SparkContext(conf)                 //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| 15/12/10 11:02:40 INFO SparkContext: Running Spark version 1.5.2
                                                  //| 15/12/10 11:02:40 WARN NativeCodeLoader: Unable to load native-hadoop librar
                                                  //| y for your platform... using builtin-java classes where applicable
                                                  //| 15/12/10 11:02:40 INFO SecurityManager: Changing view acls to: Administrator
                                                  //| 
                                                  //| 15/12/10 11:02:40 INFO SecurityManager: Changing modify acls to: Administrat
                                                  //| or
                                                  //| 15/12/10 11:02:40 INFO SecurityManager: SecurityManager: authentication disa
                                                  //| bled; ui acls disabled; users with view permissions: Set(Administrator); use
                                                  //| rs with modify permissions: Set(Administrator)
                                                  //| 15/12/10 11:02:40 INFO Slf4jLogger: Slf4jLogger started
                                                  //| 15/12/10 11:02:40 INFO Remoting: Starting remoting
                                                  //| 15/12/10 11:02:41 ERROR NettyTransport: failed to bind to /192.168.27.60:0, 
                                                  //| shutting down Netty transport
                                                  //| 15/12/10 11:02:41 WARN Utils: Service 'sparkDriver' could no
                                                  //| Output exceeds cutoff limit.
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    
  
    val df=sqlContext.jsonFile("/user/liujiyu/spark/finalresultstatistics.json")
    
		//df.filter(df("name")>21).show()
    
    df.printSchema()
}