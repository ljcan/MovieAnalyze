package cu.just.spark.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import cu.just.spark.domain.GradeCount;
import cu.just.spark.domain.WordCount;
import cu.just.spark.utils.MysqlUtil;
@Component
public class WordCountDao {
	
	public List<WordCount> wordList(){
		List<WordCount> list=new ArrayList<WordCount>(); 
		Connection conn=MysqlUtil.getConnection();
		ResultSet rs=null;
		try {
			PreparedStatement pstm=conn.prepareStatement("select * from wordcount limit 200");
			rs=pstm.executeQuery();
			while(rs.next()) {
				WordCount count=new WordCount();
				count.setWord(rs.getString(1));
				count.setCount(rs.getInt(2));
				list.add(count);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				rs.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return list;
	}
	
	public List<GradeCount> gradeList(){
		List<GradeCount> list=new ArrayList<GradeCount>(); 
		Connection conn=MysqlUtil.getConnection();
		ResultSet rs=null;
		try {
			PreparedStatement pstm=conn.prepareStatement("select * from moviegrade");
			rs=pstm.executeQuery();
			while(rs.next()) {
				GradeCount count=new GradeCount();
				count.setGrade(rs.getString(1));
				count.setCount(rs.getInt(2));
				list.add(count);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				rs.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return list;
	}
	
//	public static void main(String[] args) {
//		List<WordCount> list=new ArrayList<WordCount>();
//		list=wordList();
//		for(WordCount wordCount:list) {
//			System.out.println(wordCount.toString());
//		}
//	}

}
