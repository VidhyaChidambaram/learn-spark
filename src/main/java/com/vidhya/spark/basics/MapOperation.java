package com.vidhya.spark.basics;

import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.vidhya.spark.builder.SparkContextBuilder;

public class MapOperation {

  public static void main(String[] args) {

    JavaSparkContext jsc = SparkContextBuilder.getInstance().getJavaSparkContext();
    String[] inputData = new String[] {"1", "2", "3"};
    JavaRDD<String> inputRdd = jsc.parallelize(Arrays.asList(inputData));
    JavaRDD<Double> outputRdd =
        inputRdd.map(entry -> Math.pow(Double.valueOf(entry), Double.valueOf(2.0)));
    outputRdd.collect().forEach(System.out::println);
  }
}
