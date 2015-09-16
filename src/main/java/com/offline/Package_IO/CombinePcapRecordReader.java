package com.offline.Package_IO;

import java.io.IOException;

import com.offline.Package_IO.PcapInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * Wrapper for CombineFileSplit to RecordReader
 * @author wnagele
 */
public class CombinePcapRecordReader implements RecordReader<LongWritable, ObjectWritable> {
	private PcapRecordReader recordReader;

	public CombinePcapRecordReader(CombineFileSplit split, Configuration conf, Reporter reporter, Integer index) throws IOException {
		Path path = split.getPath(index);
		long start = 0L;
		long length = split.getLength(index);
		recordReader = PcapInputFormat.initPcapRecordReader(path, start, length, reporter, conf);
	}

	public boolean next(LongWritable key, ObjectWritable value) throws IOException {
		return recordReader.next(key, value);
	}

	public LongWritable createKey() {
		return recordReader.createKey();
	}

	public ObjectWritable createValue() {
		return recordReader.createValue();
	}

	public long getPos() throws IOException {
		return recordReader.getPos();
	}

	public void close() throws IOException {
		recordReader.close();
	}

	public float getProgress() throws IOException {
		return recordReader.getProgress();
	}
}