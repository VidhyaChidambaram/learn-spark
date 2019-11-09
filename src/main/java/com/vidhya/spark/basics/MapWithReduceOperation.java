package com.vidhya.spark.basics;

import java.util.Arrays;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.vidhya.spark.builder.SparkContextBuilder;

/**
 * Basic use case - map and reduce
 * 
 * @author vidhy
 *
 */
public class MapWithReduceOperation {

  private static final Logger LOGGER = Logger.getLogger(MapWithReduceOperation.class);

  public static void main(String[] args) {
    Integer[] input = new Integer[] {1, 2, 3, 4, 5};
    JavaSparkContext jsc = SparkContextBuilder.getInstance().getJavaSparkContext();
    JavaRDD<Integer> inputRdd = jsc.parallelize(Arrays.asList(input));
    final Integer value = reduceOperation(inputRdd);
    LOGGER.info("The final output is " + value);
  }

  private static Integer reduceOperation(JavaRDD<Integer> inputRdd) {
    return inputRdd.map(entry -> entry * entry).reduce((val1, val2) -> val1 * val2);
  }

}
