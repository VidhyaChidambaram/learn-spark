package com.vidhya.spark.basics;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import com.vidhya.spark.builder.SparkContextBuilder;
import scala.Tuple2;

/**
 * Load a file containing many lines of logs, and count the number of occurences of different types
 * of LOG levels like this : {INFO, 3}, {DEBUG, 5} etc
 * 
 * @author vidhy
 *
 */
public class PairRDDExample {

  private static final Logger LOGGER = Logger.getLogger(PairRDDExample.class);

  public static void main(String[] args) {
    SparkSession sparkSession = SparkContextBuilder.getInstance().getSparkSession();
    JavaRDD<String> fileData =
        sparkSession.read().textFile("src/main/resources/PairRddExampleTestFile.txt").toJavaRDD();

    fileData.mapToPair(line -> new Tuple2<>(line.split(" ")[2], 1))
        .reduceByKey((value1, value2) -> value1 + value2).foreach(line -> LOGGER
            .info("The log type is : " + line._1 + "and number of occurences is :" + line._2));
  }
}
