package cn.just.shinelon.movie;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setMaster("local[4]")
				.setAppName("WordCount");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> wordsRDD=sc.textFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/movie/wordgrade.txt");
	

		
		JavaRDD<String> flatRDD=wordsRDD.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		
		//计算词频数
		JavaPairRDD<String, Integer> wordMapRDD=wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
//		JavaPairRDD<String, Integer> filterRDD=wordMapRDD.filter(new Function<Tuple2<String,Integer>, Boolean>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
//				if(v1._1.equals("")) {
//					return false;
//				}
//				return true;
//			}
//		});
		
		JavaPairRDD<String, Integer> wordcount=wordMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		JavaPairRDD<Integer, String> countwordRDD=wordcount.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2, t._1);
			}
		});
		
		JavaPairRDD<Integer, String> resultRDD=countwordRDD.sortByKey(false);
		
		resultRDD.foreach(new VoidFunction<Tuple2<Integer,String>>() {

			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<Integer,String> t) throws Exception {
					System.out.println(t._2+"  "+t._1);
			}
		});
		
		
	}

}
