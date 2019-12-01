package com.vidhya.spark.intermediate;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.vidhya.spark.builder.SparkContextBuilder;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Program to calculate average salary paid out for each month per department based on number of
 * employees
 * 
 * @author vidhy
 *
 */
public class SparkRightOuterJoins {

  public static void main(String[] args) {

    JavaPairRDD<String, String> departmentPair =
        convertFileContentToPairRdd("src/main/resources/rightouterjoin/Departments.txt");
    JavaPairRDD<String, String> salaryPair =
        convertFileContentToPairRdd("src/main/resources/rightouterjoin/AverageSalary.txt");
    JavaPairRDD<String, Tuple2<Optional<String>, String>> joinedOutput =
        departmentPair.rightOuterJoin(salaryPair);
    JavaRDD<Tuple3<String, String, String>> withReplacement =
        joinedOutput.map(entry -> new Tuple3<String, String, String>(entry._1,
            entry._2._1.orElse("NA"), entry._2._2));
    withReplacement.collect().forEach(System.out::println);
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
