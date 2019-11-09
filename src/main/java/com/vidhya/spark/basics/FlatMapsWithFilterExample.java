package com.vidhya.spark.basics;

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import com.vidhya.spark.builder.SparkContextBuilder;
import scala.Tuple2;

/**
 * Count alphabet words from a given log file whose occurence is more than one
 * 
 * @author vidhy
 *
 */
public class FlatMapsWithFilterExample {

  private static final Logger LOGGER = Logger.getLogger(FlatMapsWithFilterExample.class);

  public static void main(String[] args) {
    SparkSession sparkSession = SparkContextBuilder.getInstance().getSparkSession();
    JavaRDD<String> inputRdd =
        sparkSession.read().textFile("src/main/resources/PairRddExampleTestFile.txt").toJavaRDD();
    JavaPairRDD<String, Integer> splitRdd =
        inputRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
            .mapToPair(line -> new Tuple2<>(line, 1));
    JavaPairRDD<String, Integer> occurenceRdd =
        splitRdd.reduceByKey((value1, value2) -> value1 + value2);
    occurenceRdd
        .foreach(record -> LOGGER.info("Word : " + record._1 + "Occurence : " + record._2()));
    LOGGER.info("Before filtering :" + occurenceRdd.count());
    JavaPairRDD<String, Integer> filteredRdd = occurenceRdd.filter(record -> record._2 > 1)
        .filter(record -> StringUtils.isAlpha(record._1));

    LOGGER.info("After filtering :" + filteredRdd.count());
  }
}
