
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortKeySortComparator extends WritableComparator {

	protected SecondarySortKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getCategory().compareTo(key2.getCategory());
		if (cmpResult == 0)// same category
		{
			return -Double.compare(Double.parseDouble(key1.getWordCSPair().split(",")[0]),
					(Double.parseDouble(key2.getWordCSPair().split(",")[0]))); // -1 for descending order
		}
		return cmpResult;
	}
}