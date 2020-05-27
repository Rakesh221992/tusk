package Spark_DF

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Spark_DF {
  
case class schema(name1:String,name2:String,company_name:String,address:String,city:String,county:String,state:String,zip:String,age:String,phone1:String,phone2:String,email:String,web:String)

def main(args:Array[String]):Unit={

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
				val sc = new SparkContext(conf)
				sc.setLogLevel("Error")
				val data_rdd = sc.textFile("file:///E:///bigdata//sai_class//Data-20190920T170815Z-001//Data//usdata.csv")
				val data_rdd_contains=data_rdd.filter(x=>x.contains("\""))

				data_rdd_contains.take(5).foreach(println)

				val data_rdd__not_contains=data_rdd.filter(x => !(x.contains("\"")))

				println
				data_rdd__not_contains.take(5).foreach(println)

				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._

				val df1=	data_rdd__not_contains.map(x=>x.split(",")).map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12))).toDF()


				val df2=	data_rdd_contains.map(x=>x.split(",")).map(x=>schema(x(0),x(1),x(2)+","+x(3).trim(),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13))).toDF()


				val finaldf= df1.union(df2)
				println
				println("final_df")
				finaldf.show(1000,false)




}
}