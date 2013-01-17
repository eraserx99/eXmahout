package com.esoteric.mahout.sample;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.NamedVector;

import com.google.common.collect.Lists;

public class ClusterDumper extends AbstractJob {
	public static final String POINTS_DIR_OPTION = "pointsDir";
	public static final String DUMP_DOCUMENT_ID = "dumpDocumentId";
	public static final String DUMP_TOP_TERMS = "dumpTopTerms";

	private String getPatentMainClassfication(String s) {
	    Pattern MY_PATTERN = Pattern.compile("(.+)-(.+).txt", Pattern.CASE_INSENSITIVE);
	    Matcher m = MY_PATTERN.matcher(s);
	    while(m.find()) {
	      return m.group(2) ;
	    }
	    
	    return new String();
	}

	@Override
	public int run(String[] args) throws Exception {
		addOption(
				POINTS_DIR_OPTION,
				"p",
				"The directory containing points sequence files mapping input vectors to their cluster.",
				true);
		addFlag(DUMP_DOCUMENT_ID, 
				"d",
				"Dump the document IDs belonging to each of the clusters");
		addFlag(DUMP_TOP_TERMS, 
				"t",
				"Dump the top terms for each of the clusters.");

		Map<String, List<String>> argMap = parseArguments(args);
		if (argMap == null) {
			return -1;
		}

		Path pointsPathDir = new Path(getOption(POINTS_DIR_OPTION));
		FileSystem fs = FileSystem.get(pointsPathDir.toUri(), getConf());
		if (fs.exists(pointsPathDir)) {
			Map<Integer, List<WeightedVectorWritable>> result = new TreeMap<Integer, List<WeightedVectorWritable>>();
			for (Pair<IntWritable, WeightedVectorWritable> record : new SequenceFileDirIterable<IntWritable, WeightedVectorWritable>(
					pointsPathDir, PathType.LIST, PathFilters.logsCRCFilter(),
					getConf())) {
				int keyValue = record.getFirst().get();
				List<WeightedVectorWritable> pointList = result.get(keyValue);
				if (pointList == null) {
					pointList = Lists.newArrayList();
					result.put(keyValue, pointList);
				}
				pointList.add(record.getSecond());
			}
			for (int k : result.keySet()) {
				StringBuilder buf = new StringBuilder();
				boolean first = true;
				buf.append(k).append("\t").append(result.get(k).size())
						.append("\t");
				buf.append("[ ");
				if (argMap.containsKey(DUMP_DOCUMENT_ID)) {
					for (WeightedVectorWritable v : result.get(k)) {
						if (v.getVector() instanceof NamedVector) {
							if (first) {
								buf.append(((NamedVector) v.getVector())
										.getName());
								first = false;
							} else {
								buf.append(", ")
										.append(((NamedVector) v.getVector())
												.getName());
							}
						} 
					} 
				}
				buf.append(" ]\t");
				System.out.println(buf);
			}
		} else {
			return -1;
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner
				.run(new Configuration(), new ClusterDumper(), args);
		System.exit(res);
	}

	/**
	 * @param args
	 */
	/*
	 * public static void main(String[] args) { try { BufferedWriter bw;
	 * Configuration conf = new Configuration(); FileSystem fs =
	 * FileSystem.get(conf); File pointsFolder = new File(args[0]); File files[]
	 * = pointsFolder.listFiles(); bw = new BufferedWriter(new FileWriter(new
	 * File(args[1]))); HashMap<String, Integer> clusterIds; clusterIds = new
	 * HashMap<String, Integer>(5000); for (File file : files) { if
	 * (file.getName().indexOf("part-m") < 0) continue; String aPath =
	 * file.getAbsolutePath(); SequenceFile.Reader reader = new
	 * SequenceFile.Reader(fs, new Path(aPath), conf); IntWritable key = new
	 * IntWritable(); WeightedVectorWritable value = new
	 * WeightedVectorWritable(); while (reader.next(key, value)) { NamedVector
	 * vector = (NamedVector) value.getVector(); String vectorName =
	 * vector.getName(); bw.write(vectorName + "\t" + key.toString() + "\t" +
	 * vector.toString() + "\n"); if (clusterIds.containsKey(key.toString())) {
	 * clusterIds.put(key.toString(), clusterIds.get(key.toString()) + 1); }
	 * else clusterIds.put(key.toString(), 1); } bw.flush(); reader.close(); }
	 * bw.flush(); bw.close(); bw = new BufferedWriter(new FileWriter(new
	 * File(args[2]))); Set<String> keys = clusterIds.keySet(); for (String key
	 * : keys) { bw.write(key + " " + clusterIds.get(key) + "\n"); } bw.flush();
	 * bw.close(); } catch (IOException e) { e.printStackTrace(); } }
	 */
}
