package cn.just.shinelon.movie;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;


import cn.just.shinelon.utils.MysqlUtil;
import scala.Tuple2;

public class MovieGrade {
	
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setMaster("local[4]")
				.setAppName("MovieGrade");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> gradeRDD=sc.textFile("E:\\Spark_Project\\MovieGrade\\src\\main\\java\\cn\\just\\shinelon\\data\\film_grade.txt");
	
		JavaPairRDD<String, Integer> gradeCountRDD=gradeRDD.mapToPair(
				new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String[] gradeCount=t.split(" ");
				return new Tuple2<String, Integer>(gradeCount[0], Integer.valueOf(gradeCount[1]));
			}
		});
		
		JavaPairRDD<String, Integer> resultRDD=gradeCountRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		resultRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
//				System.out.println(t._1+"   "+t._2);
				insert(t);
			}
		});
		
		
		
		
		
	}
	
	public static void insert(Tuple2<String, Integer> t) {
		Connection connection=MysqlUtil.getConnection();
		PreparedStatement pstm=null;
	    try {
	    	pstm=connection.prepareStatement("insert into moviegrade values(?,?)");
	    	pstm.setString(1,t._1);
			pstm.setInt(2,t._2);
			pstm.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    MysqlUtil.release();
	}

}
