package com.spark.redis

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession, redis}


object SparkRedisApp {

  def main(args: Array[String]): Unit = {
    println("WOrking!!")

    val spark = SparkSession
      .builder()
      .appName("redis-df")
      .master("local[*]")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")


    import spark.implicits._
    val columns = Seq("language","users", "count")
    val data = Seq(("Java","sunil", "20000"), ("Python","kiran", "100000"), ("Scala","krishna", "3000"))

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns:_*)
    df.printSchema()
    df.show()



    val writeDF = df.filter("count <=20000")

    writeDF.write
      .format("org.apache.spark.sql.redis")
      .option("table", "students")
      .mode(SaveMode.Overwrite)
      .save()


    val readDF = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "students")
      .load()

    readDF.show()

    spark.sql(
      s"""CREATE TEMPORARY VIEW student (language STRING, users STRING, count int)
           USING org.apache.spark.sql.redis OPTIONS (table 'students')
         """)


    val readSQLDF = spark.sql(s"SELECT * FROM student")
    readSQLDF.printSchema()
    readSQLDF.show()



    val orcDF = spark.read.orc("/user/data/orc/")

    orcDF.printSchema()

    orcDF.show()

    orcDF.write
          .format("org.apache.spark.sql.redis")
          .option("table", "custom")
          .option("key.column","custom_date")
          .mode(SaveMode.Overwrite)
          .save()


//    val sensors = spark
//      .readStream
//      .format("redis")                          // read from Redis
//      .option("stream.keys", "users")         // stream key
//      .schema(StructType(Array(                 // stream fields
//        StructField("language", StringType),
//        StructField("count", IntegerType)
//      )))
//      .load()
//
//    val query = sensors
//      .writeStream
//      .format("console")
//      .start()
//
//
//    query.awaitTermination()
  }
}
