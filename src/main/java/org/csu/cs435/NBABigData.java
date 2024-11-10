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
        if(args.length != 1) {
            System.err.println("Usage: NBABigData <play-by-play.csv>");
        }
        // todo add implementation
    }
}