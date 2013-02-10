/**
 * 
 */
package com.esoteric.mahout.sample;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterWriter;
import org.apache.mahout.utils.clustering.GraphMLClusterWriter;
import org.apache.mahout.utils.vectors.VectorHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

public final class TFIDFDumper extends AbstractJob {

	protected DistanceMeasure measure;

	public static final String OUTPUT_OPTION = "output";
	public static final String DICTIONARY_TYPE_OPTION = "dictionaryType";
	public static final String DICTIONARY_OPTION = "dictionary";
	public static final String NUM_WORDS_OPTION = "numWords";
	public static final String SUBSTRING_OPTION = "substring";
	public static final String SEQ_FILE_DIR_OPTION = "seqFileDir";

	private static final Logger log = LoggerFactory
			.getLogger(ClusterDumper.class);
	private Path seqFileDir;
	private String termDictionary;
	private String dictionaryFormat;
	private int subString = Integer.MAX_VALUE;
	private int numTopFeatures = 10;

	public TFIDFDumper(Path seqFileDir, Path pointsDir) {
		this.seqFileDir = seqFileDir;
	}

	public TFIDFDumper() {
		setConf(new Configuration());
	}

	public static void main(String[] args) throws Exception {
		new TFIDFDumper().run(args);
	}

	@Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		addOption(SUBSTRING_OPTION, "b",
				"The number of chars of the asFormatString() to print");
		addOption(NUM_WORDS_OPTION, "n", "The number of top terms to print");
		addOption(DICTIONARY_OPTION, "d", "The dictionary file");
		addOption(DICTIONARY_TYPE_OPTION, "dt",
				"The dictionary file type (text|sequencefile)", "sequencefile");
		addOption(DefaultOptionCreator.distanceMeasureOption().create());
		if (parseArguments(args) == null) {
			return -1;
		}

		seqFileDir = getInputPath();
		outputFile = getOutputFile();
		if (hasOption(SUBSTRING_OPTION)) {
			int sub = Integer.parseInt(getOption(SUBSTRING_OPTION));
			if (sub >= 0) {
				subString = sub;
			}
		}
		termDictionary = getOption(DICTIONARY_OPTION);
		dictionaryFormat = getOption(DICTIONARY_TYPE_OPTION);
		if (hasOption(NUM_WORDS_OPTION)) {
			numTopFeatures = Integer.parseInt(getOption(NUM_WORDS_OPTION));
		}
		String distanceMeasureClass = getOption(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
		measure = ClassUtils.instantiateAs(distanceMeasureClass,
				DistanceMeasure.class);

		dumpTFIDFs();
		return 0;
	}

	public void dumpTFIDFs() throws Exception {
		Configuration conf = new Configuration();
		String[] dictionary = null;

		if (this.termDictionary != null) {
			if ("text".equals(dictionaryFormat)) {
				dictionary = VectorHelper.loadTermDictionary(new File(
						this.termDictionary));
			} else if ("sequencefile".equals(dictionaryFormat)) {
				dictionary = VectorHelper.loadTermDictionary(conf,
						this.termDictionary);
			} else {
				throw new IllegalArgumentException("Invalid dictionary format");
			}
		}
		
		for (Pair<Text, VectorWritable> record : new SequenceFileDirIterable<Text, VectorWritable>(
				seqFileDir, PathType.LIST, PathFilters.logsCRCFilter(),
				conf)) {
			Text key = record.getFirst();
		    Vector vector = record.getSecond().get();
		    String name = null;
		    if(vector instanceof NamedVector) {
		    		name = ((NamedVector)vector).getName();
		    }
		    List<Pair<Integer, Double>> lpID = VectorHelper.firstEntries(vector, Integer.MAX_VALUE);
		    List<Pair<String, Double>> lpSD = VectorHelper.toWeightedTerms(lpID, dictionary);
		    int i = 0;
		    log.info("Name => " + name);
		    for(Pair<String, Double> sd : lpSD) {
		    		log.info(sd.getFirst());
		    		++i;
		    }
		} 
	}

	ClusterWriter createClusterWriter(Writer writer, String[] dictionary)
			throws IOException {
		ClusterWriter result = null;

		/*
		 * result = new ClusterDumperWriter(writer, clusterIdToPoints, measure,
		 * numTopFeatures, dictionary, subString);
		 */
		return result;
	}

	public int getSubString() {
		return subString;
	}

	public void setSubString(int subString) {
		this.subString = subString;
	}

	public String getTermDictionary() {
		return termDictionary;
	}

	public void setTermDictionary(String termDictionary, String dictionaryType) {
		this.termDictionary = termDictionary;
		this.dictionaryFormat = dictionaryType;
	}

	public void setNumTopFeatures(int num) {
		this.numTopFeatures = num;
	}

	public int getNumTopFeatures() {
		return this.numTopFeatures;
	}
}
