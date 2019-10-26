package com.vidhya.spark.builder;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkContextBuilder {

  private static SparkContextBuilder sparkContextBuilder;
  private JavaSparkContext javaSparkContext;
  private SparkSession sparkSession;


  private SparkContextBuilder() {}

  /**
   * Get Spark SQL Context from existing spark session obejct
   * 
   * @return
   */
  public SQLContext getSparkSQLContext() {
    return getSparkSession().sqlContext();
  }

  /**
   * Get Java Spark Context from spark session
   * 
   * @return
   */
  public JavaSparkContext getJavaSparkContext() {
    if (javaSparkContext == null) {
      this.javaSparkContext = new JavaSparkContext(getSparkSession().sparkContext());
    }
    return javaSparkContext;
  }

  /**
   * Create an instance of spark Session
   * 
   * @return
   */
  public SparkSession getSparkSession() {
    if (this.sparkSession == null) {
      this.sparkSession =
          SparkSession.builder().appName("learn-spark").master("local").getOrCreate();
    }
    return sparkSession;
  }

  /**
   * Create an instance of spark context builder, singleton
   * 
   * @return
   */
  public static SparkContextBuilder getInstance() {
    if (sparkContextBuilder == null) {
      sparkContextBuilder = new SparkContextBuilder();
    }
    return sparkContextBuilder;
  }
}
