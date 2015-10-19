/*
 * Decompiled with CFR 0_102.
 * 
 * Could not load the following classes:
 *  org.apache.hadoop.conf.Configuration
 *  org.apache.hadoop.fs.FileSystem
 *  org.apache.hadoop.fs.Path
 *  org.apache.hadoop.io.BytesWritable
 *  org.apache.hadoop.io.LongWritable
 *  org.apache.hadoop.io.compress.CompressionCodec
 *  org.apache.hadoop.io.compress.CompressionCodecFactory
 *  org.apache.hadoop.mapred.FileInputFormat
 *  org.apache.hadoop.mapred.FileSplit
 *  org.apache.hadoop.mapred.InputSplit
 *  org.apache.hadoop.mapred.JobConf
 *  org.apache.hadoop.mapred.JobConfigurable
 *  org.apache.hadoop.mapred.RecordReader
 *  org.apache.hadoop.mapred.Reporter
 */
package com.offline.Package_IO;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.offline.Package_IO.PcapVlenRecordReader;

public class PcapInputFormat
extends FileInputFormat<LongWritable, BytesWritable>
implements JobConfigurable {
    private CompressionCodecFactory compressionCodecs = null;

    public void configure(JobConf conf) {
        this.compressionCodecs = new CompressionCodecFactory((Configuration)conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        if (this.compressionCodecs.getCodec(file) == null) {
            return true;
        }
        return false;
    }

    public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new PcapVlenRecordReader((Configuration)job, (FileSplit)genericSplit);
    }
}

