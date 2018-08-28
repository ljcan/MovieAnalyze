package cn.just.shinelon.movie;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 数据清洗工具
 * @author shinelon
 *
 */
public class DataFormatUtil {
	
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setMaster("local[4]")
				.setAppName("DataFormatUtil");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> filmRDD=sc.textFile("E:\\Spark_Project\\MovieGrade\\src\\main\\java\\cn\\just\\shinelon\\data\\film.txt");
		
		
	}

}
