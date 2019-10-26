package com.vidhya.spark.basics;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.vidhya.spark.builder.SparkContextBuilder;

public class ParallelizeOperation {

  public static void main(String[] args) {
    String[] inputData = new String[] {"1", "2", "3"};
    JavaSparkContext jsc = SparkContextBuilder.getInstance().getJavaSparkContext();
    JavaRDD<String> javaRDD = jsc.parallelize(Arrays.asList(inputData));
    System.out.println("Total Elements in list: " + javaRDD.count());
    System.out.println("Done");
  }
}
