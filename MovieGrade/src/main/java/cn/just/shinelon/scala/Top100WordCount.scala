package cn.just.shinelon.Movie

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Top100WordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("Top100WordCount")

    val sc=new SparkContext(conf)

    //读取分词后的数据文件
    val text=sc.textFile("hdfs://hadoop-senior.shinelon.com:8020/user/shinelon/movie/wordgrade.txt")
    val top100Word=text.flatMap(_.split(" ")).map((_,1)).filter(x=> !x._1.equals(""))
      .reduceByKey(_+_).sortBy(_._2,false).take(500)
    //插入数据库
    insert(top100Word)


  }

  def insert(word:Array[(String,Int)]): Unit ={
    val connection=MysqlUtil.getConnection()
    connection.setAutoCommit(false)
    val pstm=connection.prepareStatement("insert into wordcount values(?,?)");
    for (elem <- word) {
      pstm.setString(1,elem._1)
      pstm.setInt(2,elem._2)
      pstm.addBatch()
    }
    pstm.executeBatch()
    connection.commit()
    MysqlUtil.release(connection,pstm)
  }




}
