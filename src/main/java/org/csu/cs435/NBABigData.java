package org.csu.cs435;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NBABigData {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: NBABigData <play-by-play.csv>");
        }
        SparkSession spark = createSparkSession();

        Dataset<String> rawNbaPlayByPlay = readData(spark, args[0]);

        System.out.println("First few rows of the Dataset:");
        nbaPlayByPlay.show(5);
    }

    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("NBA Play-by-Play Data Processing")
                .master("local")
                .getOrCreate();
    }

    private static Dataset<String> readData(final SparkSession spark, final String filePath) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);
    }
}