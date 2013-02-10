package com.esoteric.mahout.sample;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.vectors.VectorHelper;

public class TFIDF2YLDA extends Configured implements Tool {

	private static String[] dictionary = null;

	/**
	 * @param conf
	 * @param termDictionary
	 * @return
	 */
	public int loadTermDictionary(Configuration conf, String termDictionary) {
		dictionary = VectorHelper.loadTermDictionary(conf, termDictionary);
		return dictionary.length;
	}

	/**
	 *
	 */
	public static class TFIDF2YLDAMapper extends
			Mapper<Text, VectorWritable, Text, NullWritable> {

		/**
		 * @param s
		 * @return
		 */
		private static String getAuxId(String s) {
			Pattern MY_PATTERN = Pattern.compile("(.+)-(.+)",
					Pattern.CASE_INSENSITIVE);
			Matcher m = MY_PATTERN.matcher(s);
			while (m.find()) {
				return m.group(2);
			}

			return new String();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce
		 * .Mapper.Context)
		 */
		@Override
		protected void setup(Context context) {
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(Text id, VectorWritable vw, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();

			String yldaId = id.toString();
			String yldaSubId = getAuxId(yldaId);

			sb.append(yldaId).append(" ").append(yldaSubId);

			List<Pair<Integer, Double>> lpID = VectorHelper.firstEntries(
					vw.get(), Integer.MAX_VALUE);
			List<Pair<String, Double>> lpSD = VectorHelper.toWeightedTerms(
					lpID, dictionary);
			for (Pair<String, Double> sd : lpSD) {
				sb.append(" ").append(sd.getFirst());
			}

			context.write(new Text(sb.toString()), NullWritable.get());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), TFIDF2YLDA.class.getName());

		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		loadTermDictionary(getConf(), args[2]);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(TFIDF2YLDAMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		/* This job does not need any reducer */
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 1 : 0;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TFIDF2YLDA(), args);
		System.exit(exitCode);
	}
}
