package com.esoteric.mahout.sample;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

public class Avro extends Configured implements Tool {

	private static final Schema SCHEMA = new Schema.Parser().parse("{"
			+ "  \"namespace\": \"com.patentagility\","
			+ "  \"type\": \"record\"," + "  \"name\": \"document\","
			+ "  \"fields\": ["
			+ "    {\"name\": \"id\", \"type\": \"string\"},"
			+ "    {\"name\": \"id-aux\", \"type\": \"string\"},"
			+ "    {\"name\": \"import-timestamp\", \"type\": \"string\"}"
			+ "    {\"name\": \"contents\", \"type\": \"string\"}" + "  ]"
			+ "}");

	public static class MyAvroMapper extends
			AvroMapper<Utf8, Pair<Text, GenericRecord>> {
		
		public void map(Utf8 line,
				AvroCollector<Pair<Text, GenericRecord>> collector,
				Reporter reporter) throws IOException {
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
