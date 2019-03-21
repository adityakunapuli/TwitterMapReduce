import opennlp.tools.stemmer.snowball.SnowballStemmer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import static opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM.ENGLISH;

public class mrPhase_Final
{
	//  We we use the following hashmap to get a total count of docs for each of the four indexes
	//  e.g. All tweets | hashtags of all tweets | tweets w/geolocation | hashtags of all tweets w/geolocation
	//	This total count is used in the third phase to calculate the inverse document frequency
	private static HashMap<String, Integer> outputHash = new HashMap<>();
	// define stopwords here (avoid repeatedly creating this same list)
	private static String[] stopWords = new String[]{"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
			"it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they",
			"this", "to", "was", "will", "with"};
	private static Set<String> stopWordsDict = new HashSet<>(Arrays.asList(stopWords));
	// initialize stemmer class here once only
	private static SnowballStemmer sbStemmer = new SnowballStemmer(ENGLISH, 1);
	//	Define custom counter here to keep track of total doc count for each index
	private static enum indexCounter
	{
		AllTweets, AllTweetsHashOnly, OnlyTweetsWithGeoHashOnly, OnlyTweetsWithGeo
	}

//  mapper1 below does a few different things:
//  it begins with reading the CSV file contain the tweets and extracts the tweet text as well as tweet ID
//	the tweet text is then cleaned up using "regExReplace" function and tokenized.  The tokens are
//	looped through and stemmed if applicable (i.e. hastags are not stemmed).  The final output will contain
//	a key composed of the term and tweetID separated by a tilde and value of one.
	public static class mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>
	{
//        	Input resembles:
//	            term~docID \t termCount
//              artist~1094839421524819968 \t 1
		private Text wordDocPair = new Text();
		private IntWritable one = new IntWritable(1);

		private String regExReplace(String textStr)
		{
			textStr = textStr.toLowerCase();
			//	Remove URLS
			textStr = textStr.replaceAll(
					"(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", "");
			//	Remove single dashes
			textStr = textStr.replaceAll("([^-])([-])([^-])", "$1$3");
			//	Replace any non-Alphanumeric repeating character with single instance
			textStr = textStr.replaceAll("(\\W)\\1+", "$1");
			//	Replace contraction of #'s with "numbers"
			textStr = textStr.replaceAll("(#'s)\\s", "numbers ");
			//	Ensure that all # and @ have a space before them to ensure tokenization
			textStr = textStr.replaceAll("([^\\s])([#]\\w+)", "$1 $2");
			//	Remove @user mentions and any non alphanumeric characters (excluding #)
			textStr = textStr.replaceAll("[@]\\w+|[@]\\W+|[^\\w#\\s]", " ");
			//	Remove dashes, single characters, and useless hashtags (e.g. #1 or #')
			textStr = textStr.replaceAll("(^| ).(( ).)*( |$)", "$1");
			textStr = textStr.replaceAll("[#][\\w\\W]\\s", "");
			//	Remove repeated spaces
			textStr = textStr.replaceAll("\\s+", " ").trim();
			return textStr;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			String param = conf.get("indexType");

			String entireTweetString = value.toString();
			String[] entireTweetArray = entireTweetString.split(",");

			String tweetID = entireTweetArray[21];
			String tweetText = regExReplace(entireTweetArray[9]); // 9 = rawtext, since it includes #tags
			String geolocation = entireTweetArray[19].trim();
//			System.out.println(tweetID + "\n" + tweetText + "\n" + geolocation);

//	        Output resembles:
//	            < docID , term=termCount >
//              < 1094839421524819968 , artist=1 >
			if ((param.equals("OnlyTweetsWithGeo") || param.equals("OnlyTweetsWithGeoHashOnly")) && geolocation.length() > 8)
			{
				// Count TOTAL number of documents for each of the four criteria
				// This is used in Phase 3 below to calculate inverse doc frequency LOG(N/n)
				if (param.equals("OnlyTweetsWithGeoHashOnly") && tweetText.contains("#"))
				{
					context.getCounter("indexCounter", param).increment(1);
				}
				if (param.equals("OnlyTweetsWithGeo"))
				{
					context.getCounter("indexCounter", param).increment(1);
				}

				StringTokenizer itr = new StringTokenizer(tweetText, " ");
				while (itr.hasMoreTokens())
				{
					String strToken = itr.nextToken().trim();
					// The following checks the type of index that's being built and that the tokens
					// meet the indexes specific conditions
					if (((param.equals("OnlyTweetsWithGeo")) && (stopWordsDict.contains(strToken)))
							|| ((param.equals("OnlyTweetsWithGeoHashOnly")) && (!strToken.substring(0, 1).equals("#"))))
					{
						continue;
					}
					CharSequence csToken = strToken;
					// exclude calls to the stemmer for hashtag tokens
					if (!strToken.substring(0, 1).equals("#"))
					{
						csToken = sbStemmer.stem(csToken);
					}
					wordDocPair.set(csToken + "~" + tweetID);
					context.write(wordDocPair, one);
				}
			}

			if (param.equals("AllTweets") || param.equals("AllTweetsHashOnly"))
			{
				// Count TOTAL number of documents for each of the four criteria
				// This is used in Phase 3 below to calculate inverse doc frequency LOG(N/n)criteria
				if (param.equals("AllTweetsHashOnly") && tweetText.contains("#"))
				{
					context.getCounter("indexCounter", param).increment(1);
				}
				if (param.equals("AllTweets"))
				{
					context.getCounter("indexCounter", param).increment(1);
				}

				StringTokenizer itr = new StringTokenizer(tweetText, " ");
				while (itr.hasMoreTokens())
				{
					String strToken = itr.nextToken().trim();
					// The following checks the type of index that's being built and that the tokens
					// meet the indexes specific conditions
					if (((param.equals("AllTweets")) && (stopWordsDict.contains(strToken)))
							|| ((param.equals("AllTweetsHashOnly")) && (!strToken.substring(0, 1).equals("#"))))
					{
						continue;
					}
					CharSequence csToken = strToken;
					// exclude calls to the stemmer for hashtag tokens
					if (!strToken.substring(0, 1).equals("#"))
					{
						csToken = sbStemmer.stem(csToken);
					}
					wordDocPair.set(csToken + "~" + tweetID);
					context.write(wordDocPair, one);
				}
			}
		}
	}

	//	reducer1 below simply counts/sums up all the incoming term~tweetID keys
	public static class reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable occurrencesOfWord = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			occurrencesOfWord.set(sum);
			context.write(key, occurrencesOfWord);
		}
	}

//	mapper2 below simply splits apart or rearranges the output from reducer1 above in the following manner:
//	<term~docID, count> -> <term, docID=count>
	public static class mapper2 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text docID = new Text();
		private Text termCount = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
//        	Input resembles:
//	            term~docID \t termCount
//              artist~1094839421524819968 \t 1
			String doc = value.toString().split("\t")[0].split("~")[1];
			String term = value.toString().split("\t")[0].split("~")[0];
			String count = value.toString().split("\t")[1];
			docID.set(doc);
			termCount.set(term + "=" + count);
//	        Output resembles:
//	            < docID , term=termCount >
//              < 1094839421524819968 , artist=1 >
			context.write(docID, termCount);
		}
	}

	//	reducer2 below gathers the word count for each tweet
	public static class reducer2 extends Reducer<Text, Text, Text, Text>
	{
		private Text termDocPair = new Text();
		private Text termFreq = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
//        	Input resembles:
//	            < docID , {term1:term1Count , term2:term2Count , ...} >
//              < 1094839421524819968 , {artist:1 , painter:4 , ...} >
			int countTermsInDoc = 0;
			Map<String, Integer> dict = new HashMap<>();
			for (Text val : values)
			{
				String term = val.toString().split("=")[0];
				String termCount = val.toString().split("=")[1];
				dict.put(term, Integer.valueOf(termCount));
				countTermsInDoc += Integer.parseInt(termCount);
			}
			for (String dictKey : dict.keySet())
			{
				termDocPair.set(dictKey + '~' + key.toString());
				termFreq.set(dict.get(dictKey) + "/" + countTermsInDoc);
//	            Output resembles:
//                  < term~docID , termFrequency >
//                  < term~docID , termCount/countOfTermsInDoc >
//                  < artist~1094839421524819968 , 1/38 >
				context.write(termDocPair, termFreq);
			}
		}
	}

	public static class mapper3 extends Mapper<LongWritable, Text, Text, Text>
	{
//      Input resembles:
//        term~docID \t termFreq
//         term~docID \t termCount/countOfTermsInDoc
//         artist~1094839421524819968 \t 1/38
		private Text term = new Text();
		private Text docTermFreqPair = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
//        	Output (note that equals sign below is purely symbolic and used to delimit the LHS/RHS of value):
//              < term , docID = termFreq >
//              < term , docID = termCount/countOfTermsInDoc >
//              < artist , 1094839421524819968 = 1/38 >
			String docID = value.toString().split("\t")[0].split("~")[1];
			String termStr = value.toString().split("\t")[0].split("~")[0];
			String termFreq = value.toString().split("\t")[1];
			term.set(termStr);
			docTermFreqPair.set(docID + "=" + termFreq);
			context.write(term, docTermFreqPair);
		}
	}

	public static class reducer3 extends Reducer<Text, Text, Text, Text>
	{
//      Input resembles:
//          < term1 , {"doc1=term1Freq" , "doc2=term1Freq" , ....} >
//          < term2 , {"doc1=term2Freq" , "doc2=term2Freq" , ....} >
//          < term3 , {"doc1=term3Freq" , "doc2=term3Freq" , ....} >
//          where doc1, doc2, ... are docIDs that contain term_i
//          < artist , {"1094839421524819968=1/38" , "1094839419117305856=2/24" , ....} >
		private Text docTerm = new Text();
		private Text valStr = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			int totalCountOfDocs = Integer.valueOf(conf.get("docCount"));
			int countOfDocsWithTerm = 0;
//          Creates an associated array resembling:
//	            term1 -> {doc1:term1Freq , doc2:term2Freq , ...}
//	            this -> {1094839420048560128=1/38, 1094839421524819968=1/37}
			Map<String, String> dict = new HashMap<>();
			for (Text val : values)
			{
				String docID = val.toString().split("=")[0];
				String termFreq = val.toString().split("=")[1];
				dict.put(docID, termFreq);
				countOfDocsWithTerm++;
			}

			for (String document : dict.keySet())
			{
				double numerator = Double.valueOf(dict.get(document).split("/")[0]);  // LHS of operand
				double denominator = Double.valueOf(dict.get(document).split("/")[1]);  // RHS of operand
				double TF = numerator / denominator;
				double iDF = (double) totalCountOfDocs / (double) countOfDocsWithTerm;
				// if doc freq = 1 then only use term-freq as Log(iDF) = Log(1) = 0
				double TFiDF = iDF == 1 ? TF : TF * Math.log10(iDF);
				docTerm.set(key + "~" + document);
				String strDocFreq = countOfDocsWithTerm + "/" + totalCountOfDocs;
				String strTermFreq = (int) numerator + "/" + (int) denominator;
				String StrTFiDF = String.format("%.10f", TFiDF);
				String StrConcat = "[" + strDocFreq + "," + strTermFreq + "," + StrTFiDF + "]";
				valStr.set(StrConcat);
				context.write(docTerm, valStr);
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		long startTime = System.currentTimeMillis();

		String OS = System.getProperty("os.name").toLowerCase();
		Configuration conf = new Configuration();
		if (OS.contains("win"))
		{
			String outputDir = java.nio.file.Paths.get("", "output").toString();
			System.out.println(outputDir);
			System.setProperty("hadoop.home.dir", "C:\\Hadoop-2.8.0");
			FileUtils.deleteDirectory(new File(outputDir));
		}
		if (!OS.contains("win"))
		{
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			FileSystem hdfs = FileSystem.get(URI.create("hdfs://10.0.8.2:8020"), conf);
			hdfs.delete(new Path("/user/hadoop8/output/"), true);
		}
		BasicConfigurator.configure();

//		Define the four index types here
		String[] indexArray = {"AllTweets", "AllTweetsHashOnly", "OnlyTweetsWithGeoHashOnly", "OnlyTweetsWithGeo"};
		String strLineTop = String.format("%0" + 100 + "d", 0).replace("0", "=") + "\n";
		String strLineBtm = "\n" + String.format("%0" + 100 + "d", 0).replace("0", "=");
		String strLineBtm2 = "\n" + String.format("%0" + 100 + "d", 0).replace("0", "_");
		String inputDir;
		String outputDir;

/*
		  _____    _                              __
		 |  __ \  | |                            /_ |
		 | |__) | | |__     __ _   ___    ___     | |
		 |  ___/  | '_ \   / _` | / __|  / _ \    | |
		 | |      | | | | | (_| | \__ \ |  __/    | |
		 |_|      |_| |_|  \__,_| |___/  \___|    |_|
*/
		System.out.println(strLineTop + "Starting Phase 1" + strLineBtm);
		for (String output : indexArray)
		{
			System.out.println("\n\t\t[Phase 1] " + output + strLineBtm2);
			inputDir = "input";
			outputDir = java.nio.file.Paths.get("", "output", "output1", output).toString();
			conf.set("indexType", output);

			Job job = Job.getInstance(conf, "Phase 1");
			FileInputFormat.addInputPath(job, new Path(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setJarByClass(mrPhase_Final.class);
			job.setMapperClass(mapper1.class);
			job.setReducerClass(reducer1.class);
			job.setCombinerClass(reducer1.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.waitForCompletion(true);

//			Pass on total document count
			Long totalCountOfDocs = job.getCounters().findCounter("indexCounter", output).getValue();
			outputHash.put(output, Long.valueOf(totalCountOfDocs).intValue());
			System.out.println(totalCountOfDocs);
		}
/*
		  _____    _                              ___
		 |  __ \  | |                            |__ \
		 | |__) | | |__     __ _   ___    ___       ) |
		 |  ___/  | '_ \   / _` | / __|  / _ \     / /
		 | |      | | | | | (_| | \__ \ |  __/    / /_
		 |_|      |_| |_|  \__,_| |___/  \___|   |____|
*/
		System.out.println(strLineTop + "Starting Phase 2" + strLineBtm);
		conf = new Configuration();
		for (String output : indexArray)
		{
			System.out.println("\n\t\t[Phase 2] " + output + strLineBtm2);
			inputDir = java.nio.file.Paths.get("", "output", "output1", output).toString();
			outputDir = java.nio.file.Paths.get("", "output", "output2", output).toString();

			conf.set("indexType", output);

			Job job2 = Job.getInstance(conf, "Phase 2");
			FileInputFormat.addInputPath(job2, new Path(inputDir));
			FileOutputFormat.setOutputPath(job2, new Path(outputDir));
			job2.setJarByClass(mrPhase_Final.class);
			job2.setMapperClass(mapper2.class);
			job2.setReducerClass(reducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.waitForCompletion(true);
		}
/*
		  _____    _                              ____
		 |  __ \  | |                            |___ \
		 | |__) | | |__     __ _   ___    ___      __) |
		 |  ___/  | '_ \   / _` | / __|  / _ \    |__ <
		 | |      | | | | | (_| | \__ \ |  __/    ___) |
		 |_|      |_| |_|  \__,_| |___/  \___|   |____/
*/
		System.out.println(strLineTop + "Starting Phase 3" + strLineBtm);
		conf = new Configuration();
		for (String output : indexArray)
		{
			System.out.println("\n\t\t[Phase 3] " + output + strLineBtm2);
			inputDir = java.nio.file.Paths.get("", "output", "output2", output).toString();
			outputDir = java.nio.file.Paths.get("", "output", "output3", output).toString();
			String docCount = outputHash.get(output).toString();
			conf.set("docCount", docCount);
			System.out.println("\t[Index Type]\t=\t" + output + "\t[Document Count]\t=\t" + docCount);

			Job job3 = Job.getInstance(conf, "Phase 3");
			FileInputFormat.addInputPath(job3, new Path(inputDir));
			FileOutputFormat.setOutputPath(job3, new Path(outputDir));
			job3.setJarByClass(mrPhase_Final.class);
			job3.setMapperClass(mapper3.class);
			job3.setReducerClass(reducer3.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.waitForCompletion(true);
		}
		long duration = (System.currentTimeMillis()- startTime);
		System.out.println(Long.valueOf(duration/1000).toString());
	}
}