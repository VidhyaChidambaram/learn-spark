package com.vidhya.spark.intermediate;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.vidhya.spark.builder.SparkContextBuilder;
import scala.Tuple2;

public class FullJoinsAndCartesians {

  public static void main(String[] args) {

    JavaPairRDD<String, String> departmentPair =
        convertFileContentToPairRdd("src/main/resources/rightouterjoin/Departments.txt");
    JavaPairRDD<String, String> salaryPair =
        convertFileContentToPairRdd("src/main/resources/rightouterjoin/AverageSalary.txt");
    JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> joinedOutput =
        departmentPair.cartesian(salaryPair);
    joinedOutput.collect().forEach(System.out::println);
  }

  private static JavaPairRDD<String, String> convertFileContentToPairRdd(String filePath) {
    SparkSession session = SparkContextBuilder.getInstance().getSparkSession();
    JavaRDD<Row> fileContent = session.read().csv(filePath).javaRDD();
    return fileContent.mapToPair(row -> {
      String[] rowArray = row.mkString(",").split(",");
      return new Tuple2<String, String>(rowArray[0], rowArray[1]);
    });
  }
}
