import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

	private String category;
	private String wordCSPair;

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String category, String wordCSPair) {
		this.category = category;
		this.wordCSPair = wordCSPair;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(category).append("\t").append(wordCSPair)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		category = WritableUtils.readString(dataInput);
		wordCSPair = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, category);
		WritableUtils.writeString(dataOutput, wordCSPair);
	}

	public int compareTo(CompositeKeyWritable objKeyPair) {
		// TODO:
		/*
		 * Note: This code will work as it stands; but when CompositeKeyWritable
		 * is used as key in a map-reduce program, it is de-serialized into an
		 * object for comapareTo() method to be invoked;
		 * 
		 * To do: To optimize for speed, implement a raw comparator - will
		 * support comparison of serialized representations
		 */
		int result = category.compareTo(objKeyPair.category);
		if (0 == result) {
			result = wordCSPair.split(",")[0].compareTo(objKeyPair.wordCSPair.split(",")[0]);
		}
		return result;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getWordCSPair() {
		return wordCSPair;
	}

	public void setWordCSPair(String wordCSPair) {
		this.wordCSPair = wordCSPair;
	}
}