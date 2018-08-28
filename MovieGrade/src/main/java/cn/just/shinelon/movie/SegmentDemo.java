package cn.just.shinelon.movie;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;

import scala.Tuple2;

public class SegmentDemo {
	
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setMaster("local[4]")
				.setAppName("SegmentDemo");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		//加载停用词
	   JavaRDD<String> stopWord=sc.textFile("E:\\Spark_Project\\MovieGrade\\src\\main\\java\\cn\\just\\shinelon\\data\\StopWord.txt");
		
//		//存放分词结果
//		List<String> bufferList=new ArrayList<String>();
//		
//		JiebaSegmenter segmenter = new JiebaSegmenter();
//	    String[] sentences =
//	        new String[] {"这是一个伸手不见五指的黑夜。我叫孙悟空，我爱北京，我爱Python和C++。", "我不喜欢日本和服。", "雷猴回归人间。",
//	                      "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作", "结果婚的和尚未结过婚的"};
//	   
//	    for (String sentence : sentences) {
//	    	
//	    	Object[] list=segmenter.process(sentence, SegMode.INDEX).toArray();
//	    	for(Object str:list) {
//	    		String words=str.toString().substring(1, str.toString().length()-1);
//	    		String[] wordList=words.split(",");
////	    		System.out.println(word[0]);
//	    		String word=wordList[0];
//	    		bufferList.add(word);
//	    	}
//	    }
//	    JavaRDD<String> wordsRDD=sc.parallelize(bufferList);
	   
	   JavaRDD<String> wordsRDD=segmentWord(sc, "E:\\Spark_Project\\MovieGrade\\src\\main\\java\\cn\\just\\shinelon\\data\\film_text.txt");
	    
	    /**
	     * 转换为<word,1>的格式
	     */
	  JavaPairRDD<String,Integer> wordsPairRDD=wordsRDD.mapToPair(
			  new PairFunction<String, String, Integer>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> call(String t) throws Exception {
			return new Tuple2<String, Integer>(t, 1);
		}
	});
	   
	   
	   JavaPairRDD<String, Integer> stopWordPairRDD=stopWord.mapToPair(
			   new PairFunction<String, String, Integer>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> call(String t) throws Exception {
			return new Tuple2<String, Integer>(t, 1);
		}
	});
	   
	   JavaPairRDD<String,Tuple2<Integer, Optional<Integer>>> joinWordRDD=wordsPairRDD.leftOuterJoin(stopWordPairRDD);
	   
	   
//	   joinWordRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<Integer,Optional<Integer>>>>() {
//		   int count=0;
//		@Override
//		public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> t) throws Exception {
//			if(count<100) {
//				if(t._2._2.isPresent()) {
//					System.out.println(t._1+"  "+t._2._1+"  "+t._2._2);	
//				}
//			}
//			
//		}
//	});
	   
	    JavaPairRDD<String,Tuple2<Integer,Optional<Integer>>> filterWordRDD=joinWordRDD.filter(
	    		new Function<Tuple2<String,Tuple2<Integer,Optional<Integer>>>, Boolean>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> tuple) throws Exception {
				Optional<Integer> optional=tuple._2._2;
				if(optional.isPresent()) {    //不为空
					return false;
				}
				return true;
			}
		});
	    /**
	     * 只保留第一个字段
	     */
	    JavaRDD<String> wordMapRDD=filterWordRDD.map(
	    		new Function<Tuple2<String,Tuple2<Integer,Optional<Integer>>>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> v1) throws Exception {
				v1._1.replaceAll("^[A-Za-z0-9]+$", " ");
				if(v1._1.equals(" ")) {
					return "";
				}
				return v1._1;
			}
		});
	    
	    wordMapRDD.saveAsTextFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/movie/wordgrade.txt");
	    
	    
//	    filterWordRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<Integer,Optional<Integer>>>>() {
//	    	
//			private static final long serialVersionUID = 1L;
//			
//			int count=0;
//			@Override
//			public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> t) throws Exception {
//				if(count<1000) {
//					System.out.println(t._1);
//					count++;
//				}
//			}
//		});
	    
	}
	
	
	
	/**
	 * 从文本中读取文件并且进行分词处理
	 * @param sc
	 * @param filePath
	 * @return
	 */
	public static JavaRDD<String> segmentWord(JavaSparkContext sc,String filePath){
		InputStreamReader reader=null;
		//存放分词结果
		List<String> bufferList=new ArrayList<String>();
		try {
			reader=new InputStreamReader(new FileInputStream(new File(filePath)));
			int count=0;
			char[] buf=new char[1024]; 
			StringBuffer buffer=new StringBuffer();
			while((count=reader.read(buf))!=-1) {
				buffer.append(buf, 0, count);
			}
			String fileText=buffer.toString();
			//jieba分词器
			JiebaSegmenter segmenter = new JiebaSegmenter();
			
			Object[] list=segmenter.process(fileText, SegMode.INDEX).toArray();
	    	for(Object str:list) {
	    		String words=str.toString().substring(1, str.toString().length()-1);
	    		String[] wordList=words.split(",");
	    		String word=wordList[0];
	    		bufferList.add(word);
	    	}
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(bufferList!=null) {
			return sc.parallelize(bufferList);
		}
			return null;
	}
	

}
