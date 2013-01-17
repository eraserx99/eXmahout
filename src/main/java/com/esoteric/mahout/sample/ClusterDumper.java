package com.esoteric.mahout.sample;

import java.util.HashMap;
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

	private String getPatentMainClassification(String s) {
		Pattern MY_PATTERN = Pattern.compile("(.+)-(.+).txt",
				Pattern.CASE_INSENSITIVE);
		Matcher m = MY_PATTERN.matcher(s);
		while (m.find()) {
			return m.group(2);
		}

		return new String();
	}

	@Override
	public int run(String[] args) throws Exception {
		// Add the mandatory -p options
		addOption(
				POINTS_DIR_OPTION,
				"p",
				"The directory containing points sequence files mapping input vectors to their cluster.",
				true);
		// Add the optional -d flag
		addFlag(DUMP_DOCUMENT_ID, 
				"d",
				"Dump the document IDs belonging to each of the clusters");
		// Add the optional -t flag
		addFlag(DUMP_TOP_TERMS, 
				"t",
				"Dump the top terms for each of the clusters.");

		Map<String, List<String>> argMap = parseArguments(args);
		if (argMap == null) {
			return -1;
		}
		
		// Obtain the points directory and loop through the
		// points files to gather the clusters and vectors matching information
		Path pointsPathDir = new Path(getOption(POINTS_DIR_OPTION));
		FileSystem fs = FileSystem.get(pointsPathDir.toUri(), getConf());
		if (fs.exists(pointsPathDir)) {
			// The points files are represented in the Hadoop sequence file format
			// The key is the cluster ID and the value is the vector presentation of the document
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
			
			// Loop through the collected clusters and dump the associated vectors
			// and other information, if requested.
			for (int k : result.keySet()) {
				Map<String, Integer> cls = new HashMap<String, Integer>();
				StringBuilder buf = new StringBuilder();
				boolean first = true;
				
				buf.append(k).append("\t").append(result.get(k).size())
						.append("\t");
				
				if (argMap.containsKey("--" + DUMP_DOCUMENT_ID)) {
					buf.append("[ ");
					for (WeightedVectorWritable v : result.get(k)) {
						if (v.getVector() instanceof NamedVector) {
							String documentId = ((NamedVector)v.getVector()).getName();
							String mainClassification = getPatentMainClassification(documentId);
							if(cls.containsKey(mainClassification)) {
								cls.put(mainClassification, cls.get(mainClassification) + 1);
							} else {
								cls.put(mainClassification, 1);
							}
							if (first) {
								buf.append(documentId);
								first = false;
							} else {
								buf.append(", ").append(documentId);
							}
						} 
					} 
					buf.append(" ]\t");
					
					first = true;
					buf.append("[ ");
					for(String cl : cls.keySet()) {
						if(first) {
							buf.append(cl).append(":").append(cls.get(cl));
							first = false;
						} else {
							buf.append(", ").append(cl).append(":").append(cls.get(cl));
						}
					}
					buf.append(" ]\t");
				}
				
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
}
