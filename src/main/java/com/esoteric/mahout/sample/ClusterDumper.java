package com.esoteric.mahout.sample;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
	public static final String PATENTS_DIR_OPTION = "patentsDir";
	public static final String DUMP_DOCUMENT_ID = "dumpDocumentId";
	public static final String DUMP_TOP_TERMS = "dumpTopTerms";
	public static final String DUMP_STATS = "dumpStats";

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
		// Add the mandatory -p option
		addOption(
				POINTS_DIR_OPTION,
				"p",
				"The directory containing points sequence files mapping input vectors to their cluster.",
				true);
		// Add the optional -t option
		addOption(PATENTS_DIR_OPTION, "a",
				"The directory + pattern of the files containing the original patent.");
		// Add the optional -d flag
		addFlag(DUMP_DOCUMENT_ID, "d",
				"Dump the document IDs belonging to each of the clusters");
		// Add the optional -t flag
		addFlag(DUMP_STATS, "s", "Dump the stats.");
		// Add the optional -t flag
		addFlag(DUMP_TOP_TERMS, "t",
				"Dump the top terms for each of the clusters.");

		Map<String, List<String>> argMap = parseArguments(args);
		if (argMap == null) {
			return -1;
		}

		FileSystem fs = null;
		Map<String, Integer> clsStat = new TreeMap<String, Integer>();

		// Obtain the patents directory and loop through the
		// patents files to get the number of patents per main classification
		String patentsDirOpt = getOption(PATENTS_DIR_OPTION);
		if (patentsDirOpt != null) {
			Path patentsPathDir = new Path(patentsDirOpt);
			fs = FileSystem.get(patentsPathDir.toUri(), getConf());
			FileStatus[] s = fs.globStatus(patentsPathDir);
			if (s == null || s.length == 0) {
				return -1;
			}
			for (int i = 0; i < s.length; ++i) {
				String mainClassification = getPatentMainClassification(s[i]
						.getPath().getName());
				if (clsStat.containsKey(mainClassification)) {
					clsStat.put(mainClassification,
							clsStat.get(mainClassification) + 1);
				} else {
					clsStat.put(mainClassification, 1);
				}
			}
		}

		// Obtain the points directory and loop through the
		// points files to gather the clusters and vectors matching information
		String pointsDirOpt = getOption(POINTS_DIR_OPTION);
		Path pointsPathDir = new Path(pointsDirOpt);
		fs = FileSystem.get(pointsPathDir.toUri(), getConf());
		if (fs.exists(pointsPathDir)) {
			// The points files are represented in the Hadoop sequence file
			// format
			// The key is the cluster ID and the value is the List of the
			// associated vector presentation of the documents
			Map<Integer, List<WeightedVectorWritable>> result = new TreeMap<Integer, List<WeightedVectorWritable>>();
			for (Pair<IntWritable, WeightedVectorWritable> record : new SequenceFileDirIterable<IntWritable, WeightedVectorWritable>(
					pointsPathDir, PathType.LIST, PathFilters.logsCRCFilter(),
					getConf())) {
				int clusterKey = record.getFirst().get();
				List<WeightedVectorWritable> pointList = result.get(clusterKey);
				if (pointList == null) {
					pointList = Lists.newArrayList();
					result.put(clusterKey, pointList);
				}
				pointList.add(record.getSecond());
			}

			// Loop through the collected clusters and dump the associated
			// vectors
			// and other information, if requested.
			for (int k : result.keySet()) {
				Map<String, Integer> clsCnt = new TreeMap<String, Integer>();
				StringBuilder buf = new StringBuilder();
				boolean first = true;

				buf.append(k).append("\t").append(result.get(k).size())
						.append("\t");

				if (argMap.containsKey("--" + DUMP_DOCUMENT_ID)) {
					buf.append("[ ");
					for (WeightedVectorWritable v : result.get(k)) {
						if (v.getVector() instanceof NamedVector) {
							// Count the number of appearances on the per
							// document basis, if DUMP_STATS is requested
							if (argMap.containsKey("--" + DUMP_STATS)) {
								String documentId = ((NamedVector) v
										.getVector()).getName();
								String mainClassification = getPatentMainClassification(documentId);
								if (clsCnt.containsKey(mainClassification)) {
									clsCnt.put(mainClassification,
											clsCnt.get(mainClassification) + 1);
								} else {
									clsCnt.put(mainClassification, 1);
								}
							}
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
					buf.append(" ]\t");
				}

				if (argMap.containsKey("--" + DUMP_STATS)) {
					if (!argMap.containsKey("--" + DUMP_DOCUMENT_ID)) {
						for (WeightedVectorWritable v : result.get(k)) {
							if (v.getVector() instanceof NamedVector) {
								// Count the number of appearances on the per
								// document basis, if DUMP_STATS is requested
								if (argMap.containsKey("--" + DUMP_STATS)) {
									String documentId = ((NamedVector) v
											.getVector()).getName();
									String mainClassification = getPatentMainClassification(documentId);
									if (clsCnt.containsKey(mainClassification)) {
										clsCnt.put(
												mainClassification,
												clsCnt.get(mainClassification) + 1);
									} else {
										clsCnt.put(mainClassification, 1);
									}
								}
							}
						}
					}
					first = true;
					buf.append("[ ");
					for (String cls : clsCnt.keySet()) {
						if (first) {
							buf.append(cls).append(":").append(clsCnt.get(cls))
									.append("/").append(clsStat.get(cls));
							first = false;
						} else {
							buf.append(", ").append(cls).append(":")
									.append(clsCnt.get(cls)).append("/")
									.append(clsStat.get(cls));
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
