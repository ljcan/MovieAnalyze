package cu.just.spark.domain;

public class WordCount {
	private String word;
	private Integer count;
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}
	@Override
	public String toString() {
		return "WordCount [word=" + word + ", count=" + count + "]";
	}
	

}
