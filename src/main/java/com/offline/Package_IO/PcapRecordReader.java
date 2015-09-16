package com.offline.Package_IO;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

import com.offline.Package_IO.PcapReader;
import com.offline.Package_IO.Packet;

import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class PcapRecordReader implements RecordReader<LongWritable, ObjectWritable> {
	PcapReader pcapReader;
	Iterator<Packet> pcapReaderIterator;
	Seekable baseStream;
	DataInputStream stream;
	Reporter reporter;

	long packetCount = 0;
	long start, end;

	public PcapRecordReader(PcapReader pcapReader, long start, long end, Seekable baseStream, DataInputStream stream, Reporter reporter) throws IOException {
		this.pcapReader = pcapReader;
		this.baseStream = baseStream;
		this.stream = stream;
		this.start = start;
		this.end = end;
		this.reporter = reporter;

		pcapReaderIterator = pcapReader.iterator();
	}

	public void close() throws IOException {
		stream.close();
	}

	public boolean next(LongWritable key, ObjectWritable value) throws IOException {
		if (!pcapReaderIterator.hasNext())
			return false;

		key.set(++packetCount);
		value.set(pcapReaderIterator.next());

		reporter.setStatus("Read " + getPos() + " of " + end + " bytes");
		reporter.progress();

		return true;
	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	public ObjectWritable createValue() {
		return new ObjectWritable();
	}

	public long getPos() throws IOException {
		return baseStream.getPos();
	}

	public float getProgress() throws IOException {
		if (start == end)
			return 0;
		return Math.min(1.0f, (getPos() - start) / (float)(end - start));
	}
}