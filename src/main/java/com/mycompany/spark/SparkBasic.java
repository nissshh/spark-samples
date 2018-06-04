/**
 * 
 */
package com.mycompany.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author dev
 *
 */
public class SparkBasic {

	JavaSparkContext sc = null;

	public SparkBasic(String appName, String master) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		sc = new JavaSparkContext(conf);
	}
	
	

	public SparkBasic(JavaSparkContext sc) {
		super();
		this.sc = sc;
	}


	public JavaPairRDD<Integer,Integer> sumOfSquares(List<Integer> data) {
		/*
		 * JavaRDD<String> lines = sc.textFile("data.txt"); JavaPairRDD<String, Integer>
		 * pairs = lines.mapToPair(s -> new Tuple2(s, 1)); JavaPairRDD<String, Integer>
		 * counts = pairs.reduceByKey((a, b) -> a + b);
		 */

		JavaRDD<Integer> distData = sc.parallelize(data);

		// returns a map of K=Number,V=Number Square
		SquareMapPair sMapPair = new SquareMapPair();
		JavaPairRDD<Integer, Integer> mapResult = distData.mapToPair(sMapPair);

		SummationSquareMapPair sumSqMapPair = new SummationSquareMapPair();
		JavaPairRDD<Integer, Integer> reduceResults = mapResult.reduceByKey(sumSqMapPair).sortByKey();
		return reduceResults;
		//String path = this.getClass().getClassLoader().getResource("results.txt").getPath();
		//reduceResults.saveAsTextFile(path);
	}

	public JavaSparkContext getSc() {
		return sc;
	}

	public void setSc(JavaSparkContext sc) {
		this.sc = sc;
	}
	
	
}
	class SquareMapPair implements PairFunction<Integer, Integer, Integer> {

		@Override
		public Tuple2<Integer, Integer> call(Integer t) throws Exception {
			
			Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(t.intValue(), (t.intValue()*t.intValue()));
			return tuple;
		}

	}

class SummationSquareMapPair implements Function2<Integer, Integer, Integer> {

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		return v1 + v2;
	}
}