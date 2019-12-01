package com.vidhya.spark.intermediate;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.vidhya.spark.builder.SparkContextBuilder;
import scala.Tuple2;
import scala.Tuple3;

public class SparkLeftOuterJoin {

  public static void main(String[] args) {

    SparkSession sparkSession = SparkContextBuilder.getInstance().getSparkSession();
    JavaRDD<Row> empIDRowRdd =
        sparkSession.read().csv("src/main/resources/leftouterjoin/IDFile.txt").toJavaRDD();
    JavaPairRDD<String, String> rowPair = empIDRowRdd.mapToPair(row -> {
      String[] rowArray = row.mkString(",").split(",");
      return new Tuple2<String, String>(rowArray[0], rowArray[1]);
    });

    JavaRDD<Row> empNameRowRdd =
        sparkSession.read().csv("src/main/resources/leftouterjoin/EmpIDMap.txt").toJavaRDD();
    JavaPairRDD<String, String> empRowPair = empNameRowRdd.mapToPair(row -> {
      String[] rowArray = row.mkString(",").split(",");
      return new Tuple2<String, String>(rowArray[0], rowArray[1]);
    });

    JavaPairRDD<String, Tuple2<String, Optional<String>>> joinedOutput =
        rowPair.leftOuterJoin(empRowPair);
    JavaRDD<Tuple3<String, String, String>> withReplacement =
        joinedOutput.map(tuple -> new Tuple3<String, String, String>(tuple._1, tuple._2._1,
            tuple._2._2.orElse("NA")));
    withReplacement.collect().forEach(System.out::println);

  }
}
