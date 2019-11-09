package com.vidhya.spark.basics;

import java.util.Arrays;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.vidhya.spark.builder.SparkContextBuilder;
import scala.Tuple2;

/**
 * Initialize a list as input, and find its sqrt using JavaRDD Usage of Tuple2 datastructure basic
 * usecase
 * 
 * @author vidhy
 *
 */
public class Tuples2Example {

  private static final Logger LOGGER = Logger.getLogger(Tuples2Example.class);

  public static void main(String[] args) {
    Integer[] input = new Integer[] {1, 2, 3, 4, 5, 6, 7, 8};
    JavaSparkContext jsc = SparkContextBuilder.getInstance().getJavaSparkContext();
    JavaRDD<Integer> inputRdd = jsc.parallelize(Arrays.asList(input));
    JavaRDD<Tuple2<Integer, Double>> tupleRDD = convertToSqrt(inputRdd);
    tupleRDD
        .foreach(line -> LOGGER.info("The Transformed : key : " + line._1 + " value :" + line._2));
  }

  private static JavaRDD<Tuple2<Integer, Double>> convertToSqrt(JavaRDD<Integer> inputRdd) {
    return inputRdd.map(entry -> new Tuple2<>(entry, Math.sqrt(entry)));
  }
}
