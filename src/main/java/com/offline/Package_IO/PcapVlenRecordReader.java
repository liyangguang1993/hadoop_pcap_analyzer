/*
 * Decompiled with CFR 0_102.
 * 
 * Could not load the following classes:
 *  org.apache.hadoop.conf.Configuration
 *  org.apache.hadoop.fs.FSDataInputStream
 *  org.apache.hadoop.fs.FileSystem
 *  org.apache.hadoop.fs.Path
 *  org.apache.hadoop.io.BytesWritable
 *  org.apache.hadoop.io.LongWritable
 *  org.apache.hadoop.io.compress.CompressionCodec
 *  org.apache.hadoop.io.compress.CompressionCodecFactory
 *  org.apache.hadoop.io.compress.CompressionInputStream
 *  org.apache.hadoop.mapred.FileSplit
 *  org.apache.hadoop.mapred.RecordReader
 */
package com.offline.Package_IO;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import com.offline.Package_IO.PcapLineReader;

public class PcapVlenRecordReader
implements RecordReader<LongWritable, BytesWritable> {
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private PcapLineReader in;
    int maxLineLength;
    private boolean fileheader_skip = true;

    public PcapVlenRecordReader(Configuration job, FileSplit split) throws IOException {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.fileheader_skip = job.getBoolean("pcap.file.header.skip", true);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        CompressionCodec codec = this.compressionCodecs.getCodec(file);
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFileHeader = false;
        boolean skipPartialRecord = false;
        int fraction = 65535*2+90;
        if (codec != null) {
            this.in = new PcapLineReader((InputStream)codec.createInputStream((InputStream)fileIn), job);
            this.end = Long.MAX_VALUE;
            skipFileHeader = true;
        } else {
            if (this.start == 0) {
                skipFileHeader = true;
            } else {
                skipPartialRecord = true;
                fileIn.seek(this.start);
            }
            this.in = new PcapLineReader((InputStream)fileIn, job);
        }
        if (skipFileHeader) {
            this.start+=(long)this.in.readFileHeader();
        }
        if (skipPartialRecord) {
            int skip = this.in.skipPartialRecord(fraction);
            while (skip == fraction) {
                this.start+=(long)skip;
                skip = this.in.skipPartialRecord(fraction);
            }
            this.start+=(long)skip;
            fileIn.seek(this.start);
            this.in = new PcapLineReader((InputStream)fileIn, job);
        }
        this.pos = this.start;
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public BytesWritable createValue() {
        return new BytesWritable();
    }

    public synchronized boolean next(LongWritable key, BytesWritable value) throws IOException {
        while (this.pos < this.end) {
            key.set(this.pos);
            int newSize = this.in.readLine(value, this.maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, this.end - this.pos), this.maxLineLength));
            if (newSize == 0) {
                this.pos = this.end;
                return false;
            }
            this.pos+=(long)newSize;
            //if (newSize >= this.maxLineLength) continue;
            if(this.pos != this.end)
                return true;
        }
        return false;
    }

    public float getProgress() {
        if (this.start == this.end) {
            return 0.0f;
        }
        return Math.min(1.0f, (float)(this.pos - this.start) / (float)(this.end - this.start));
    }

    public synchronized long getPos() throws IOException {
        return this.pos;
    }

    public synchronized void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }
}

