import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class MapReduce {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private enum CountersEnum {
			N
		}

		private final JSONParser parser = new JSONParser();
		private Set<String> patternsToSkip = new HashSet<String>();
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private ArrayList<String> uniqueWordsPerReview;

		private Configuration conf;
		private BufferedReader fis;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			if (conf.getBoolean("wordcount.skip.patterns", false)) {
				URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
				for (URI patternsURI : patternsURIs) {
					Path patternsPath = new Path(patternsURI.getPath());
					String patternsFileName = patternsPath.getName().toString();
					parseSkipFile(patternsFileName);
				}
			}
		}

		private void parseSkipFile(String fileName) {
			try {
				fis = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println(
						"Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			JSONObject jsonObj;
			try {
				jsonObj = (JSONObject) parser.parse(value.toString());
			} catch (org.json.simple.parser.ParseException e) {
				e.printStackTrace();
				return;
			}
			String category = jsonObj.get("category").toString().trim();
			String reviewText = jsonObj.get("reviewText").toString().toLowerCase();

			context.getCounter(CountersEnum.N).increment(1);
			context.write(new Text("categoryCount~" + category), one);

			uniqueWordsPerReview = new ArrayList<String>();

			reviewText = reviewText.replaceAll("\\d+", " ");
			String delimiters = " \t.!?,;:()[]{}-\"`~#&*%$\\/";
			StringTokenizer itr = new StringTokenizer(reviewText, delimiters);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken().trim());
				if (word.getLength() <= 1 || patternsToSkip.contains(word.toString())
						|| uniqueWordsPerReview.contains(word.toString())) {
					continue;
				}
				uniqueWordsPerReview.add(word.toString());
				context.write(new Text("wordCount~" + word.toString()), one);
				context.write(new Text(category + "," + word.toString()), one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class SortTopNMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

		double N = 0;
		private Configuration conf;
		private BufferedReader fis;
		private static Map<String, Integer> words = new HashMap<String, Integer>();
		private static Map<String, Integer> categories = new HashMap<String, Integer>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			N = conf.getDouble("N.value", 0);
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
			Path patternsPath = new Path(patternsURIs[0].getPath());
			String patternsFileName = patternsPath.getName().toString();
			parseFile(patternsFileName);
		}

		private void parseFile(String fileName) {
			try {
				fis = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					String key = pattern.split("\t")[0].trim();
					Integer value = Integer.parseInt(pattern.split("\t")[1].trim());
					if (pattern.contains("wordCount~")) {
						words.put(key.split("~")[1], value);
					} else if (pattern.contains("categoryCount~")) {
						categories.put(key.split("~")[1], value);
					}
				}
			} catch (IOException ioe) {
				System.err.println(
						"Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (!value.toString().contains("wordCount~") && !value.toString().contains("categoryCount~")) {

				String keyPairs = value.toString().split("\t")[0].trim();
				double cCount = categories.containsKey(keyPairs.split(",")[0].trim())
						? categories.get(keyPairs.split(",")[0].trim()) : 0;
				double wCount = words.containsKey(keyPairs.split(",")[1].trim())
						? words.get(keyPairs.split(",")[1].trim()) : 0;

				double A = Double.parseDouble(value.toString().split("\t")[1].trim());
				double B = wCount - A;
				double C = cCount - A;
				double D = N - A - B - C;
				double chi_square = N * Math.pow(((A * D) - (B * C)), 2) / ((A + B) * (A + C) * (B + D) * (C + D));

				context.write(new CompositeKeyWritable(keyPairs.split(",")[0].trim(),
						chi_square + "," + keyPairs.split(",")[1].trim()), NullWritable.get());
			}
		}
	}

	public static class SortTopNReducer extends Reducer<CompositeKeyWritable, NullWritable, Text, NullWritable> {

		private int TopN;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			TopN = context.getConfiguration().getInt("top.n.values", 0);
		}

		public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			String word_chi_squared_tuple = "";
			for (@SuppressWarnings("unused")
			NullWritable value : values) {
				if (count < TopN) {
					word_chi_squared_tuple += key.getWordCSPair().split(",")[1] + ":"
							+ key.getWordCSPair().split(",")[0] + " ";
				}
				count++;
			}
			context.write(new Text(key.getCategory() + " " + word_chi_squared_tuple.trim()), NullWritable.get());
		}
	}

	public static class SortWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] keyPairs = value.toString().split(" ");
			keyPairs = Arrays.copyOfRange(keyPairs, 1, keyPairs.length);
			for (String word : keyPairs)
				context.write(new Text(word.split(":")[0]), new IntWritable(1));
		}
	}

	public static class SortWordsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

		String one_line = "";

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			one_line += key.toString() + " ";
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(one_line.toString()), NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();

		FileSystem fs;
		fs = FileSystem.get(conf);
		int topN = 0;
		double N = 0;
		String job_one_output = "/user/e11946218/tmp";
		List<String> otherArgs = new ArrayList<String>();
		{
			Job job1 = Job.getInstance(conf, "Step 1: Tokenization, Case folding, Stopwords");
			job1.setJarByClass(MapReduce.class);
			job1.setMapperClass(TokenizerMapper.class);
			job1.setCombinerClass(IntSumReducer.class);
			job1.setReducerClass(IntSumReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);

			for (int i = 0; i < remainingArgs.length; ++i) {
				if ("-skip".equals(remainingArgs[i])) {
					job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
					job1.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				} else if ("-top".equals(remainingArgs[i])) {
					topN = Integer.parseInt(remainingArgs[++i].toString());
				} else {
					otherArgs.add(remainingArgs[i]);
				}
			}

			// Before running the job, delete the output files
			fs.delete(new Path(job_one_output), true);
			fs.delete(new Path(otherArgs.get(1)), true);

			FileInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
			FileOutputFormat.setOutputPath(job1, new Path(job_one_output));

			System.out.println("RUNNING: " + job1.getJobName());
			job1.waitForCompletion(true);
			N = job1.getCounters().findCounter(TokenizerMapper.CountersEnum.N).getValue();
		}

		{
			Job job2 = Job.getInstance(conf, "Step 2: Chi-Square Calculation, Sort and get Top N");
			job2.setJarByClass(MapReduce.class);
			job2.setMapperClass(SortTopNMapper.class);
			job2.setReducerClass(SortTopNReducer.class);
			job2.setPartitionerClass(SecondarySortPartitioner.class);
			job2.setSortComparatorClass(SecondarySortKeySortComparator.class);
			job2.setGroupingComparatorClass(SecondarySortGroupComparator.class);
			job2.setMapOutputKeyClass(CompositeKeyWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(NullWritable.class);

			job2.getConfiguration().setInt("top.n.values", topN);
			job2.getConfiguration().setDouble("N.value", N);
			job2.addCacheFile(new Path(job_one_output + "/part-r-00000").toUri());

			FileInputFormat.addInputPath(job2, new Path(job_one_output));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs.get(1)));

			System.out.println("RUNNING: " + job2.getJobName());
			job2.waitForCompletion(true);
		}

		if (fs.exists(new Path(job_one_output))) {
			fs.delete(new Path(job_one_output), true);
		}

		{
			String one_line_file = otherArgs.get(1) + "/one_line_file";
			Job job3 = Job.getInstance(conf,
					"Step 3: Creating line of all terms space-separated and ordered alphabetically");
			job3.setJarByClass(MapReduce.class);
			job3.setMapperClass(SortWordsMapper.class);
			job3.setReducerClass(SortWordsReducer.class);
			job3.setMapOutputValueClass(IntWritable.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job3, new Path(otherArgs.get(1)));
			FileOutputFormat.setOutputPath(job3, new Path(one_line_file));

			System.out.println("RUNNING: " + job3.getJobName());
			System.exit(job3.waitForCompletion(true) ? 0 : 1);
		}
	}

}