package com.esoteric.mahout.sample;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.utils.io.ChunkedWriter;

import com.google.common.io.Closeables;

public final class AvroPrefixAdditionFilter extends
		SequenceFilesFromDirectoryFilter {

	private static final Schema SCHEMA = new Schema.Parser().parse("{"
			+ "  \"namespace\": \"com.patentagility\","
			+ "  \"type\": \"record\"," + "  \"name\": \"document\","
			+ "  \"fields\": ["
			+ "    {\"name\": \"id\", \"type\": \"string\"},"
			+ "    {\"name\": \"id_aux\", \"type\": \"string\"},"
			+ "    {\"name\": \"import_timestamp\", \"type\": \"string\"},"
			+ "    {\"name\": \"contents\", \"type\": \"string\"}" + "  ]"
			+ "}");

	/**
	 * @param conf 
	 * @param keyPrefix
	 * @param options
	 * @param writer
	 * @param charset
	 * @param fs
	 */
	public AvroPrefixAdditionFilter(Configuration conf, String keyPrefix,
			Map<String, String> options, ChunkedWriter writer, Charset charset,
			FileSystem fs) {
		super(conf, keyPrefix, options, writer, charset, fs);
	}

	/* (non-Javadoc)
	 * @see com.esoteric.mahout.sample.SequenceFilesFromDirectoryFilter#process(org.apache.hadoop.fs.FileStatus, org.apache.hadoop.fs.Path)
	 */
	@Override
	protected void process(FileStatus fst, Path current) throws IOException {
		FileSystem fs = getFs();
		ChunkedWriter writer = getWriter();

		if (fst.isDir()) {
			String dirPath = getPrefix() + Path.SEPARATOR + current.getName()
					+ Path.SEPARATOR + fst.getPath().getName();
			fs.listStatus(fst.getPath(), new AvroPrefixAdditionFilter(
					getConf(), dirPath, getOptions(), writer, getCharset(), fs));
		} else {
			SeekableInput input = new FsInput(fst.getPath(), getConf());
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
					SCHEMA);
			FileReader<GenericRecord> fileReader = DataFileReader.openReader(
					input, reader);

			String n1 = fst.getPath().getName();
			String n2 = current.getName();

			try {
				while (fileReader.hasNext()) {
					GenericRecord gr = fileReader.next();
					String prefix = getPrefix();
					String name = gr.get("id").toString();
					String contents = gr.get("contents").toString();
					writer.write(prefix + Path.SEPARATOR + name, contents);
				}
			} finally {
				Closeables.closeQuietly(fileReader);
			}
		}
	}
}
