/*
 * Decompiled with CFR 0_102.
 * 
 * Could not load the following classes:
 *  org.apache.hadoop.conf.Configuration
 *  org.apache.hadoop.io.BytesWritable
 */
package com.offline.Package_IO;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import com.offline.Package_IO.BinaryUtils;
import com.offline.Package_IO.Bytes;

public class PcapLineReader {

    private long min_captime;
    private long max_captime;
    private InputStream in;
    private byte[] buffer;
    byte[] pcap_header;
    private int bufferLength = 0;
    int consumed = 0;

    public PcapLineReader(InputStream in, long min_captime, long max_captime) {
        this.in = in;
        this.buffer = new byte[65535+30];
        this.min_captime = 1403981820;//min_captime;
        this.max_captime = 1443171487;//max_captime;
    }

    public PcapLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, conf.getLong("pcap.file.captime.min", 1000000000), conf.getLong("pcap.file.captime.max", 2000000000));
    }

    public void close() throws IOException {
        this.in.close();
    }

    /*
     * Unable to fully structure code
     * Enabled aggressive block sorting
     * Lifted jumps to return sites
     */
    int skipPartialRecord(int fraction) throws IOException {
        int pos = 0;
        byte[] captured = new byte[fraction];
        byte[] tmpTimestamp1 = new byte[4];
        byte[] tmpTimestamp2 = new byte[4];
        byte[] tmpCapturedLen1 = new byte[4];
        byte[] tmpWiredLen1 = new byte[4];
        byte[] tmpCapturedLen2 = new byte[4];
        byte[] tmpWiredLen2 = new byte[4];
        int caplen1 = 0;
        long  wiredlen1 = 0;
        int  caplen2 = 0;
        long  wiredlen2 = 0;
        long  timestamp1 = 0;
        long  timestamp2 = 0;
        int  size = 0;
        long endureTime = 100;
        size = this.in.read(captured);
        
        while(pos < size){
        	
            if (size - pos < 53) {
                return size;
            }
            System.arraycopy(captured, pos, tmpTimestamp1, 0, 4);
            timestamp1 = Bytes.toLong(BinaryUtils.flipBO(tmpTimestamp1, 4));
            System.arraycopy(captured, pos + 8, tmpCapturedLen1, 0, 4);
            caplen1 = Bytes.toInt(BinaryUtils.flipBO(tmpCapturedLen1, 4));
            System.arraycopy(captured, pos + 12, tmpWiredLen1, 0, 4);
            wiredlen1 = Bytes.toInt(BinaryUtils.flipBO(tmpWiredLen1, 4));
            
            if (caplen1 > 53 && caplen1 < 65535 && size - pos - 32 - caplen1 > 0 && timestamp1 >= this.min_captime && timestamp1 < this.max_captime && wiredlen1 > 53 && wiredlen1 < 65535 && caplen1 <= wiredlen1) {
                System.arraycopy(captured,  (pos + 16 + caplen1 + 8), tmpCapturedLen2, 0, 4);
                caplen2 = Bytes.toInt(BinaryUtils.flipBO(tmpCapturedLen2, 4));
                System.arraycopy(captured,  (pos + 16 + caplen1 + 12), tmpWiredLen2, 0, 4);
                wiredlen2 = Bytes.toInt(BinaryUtils.flipBO(tmpWiredLen2, 4));
                System.arraycopy(captured, pos + 16 + caplen1, tmpTimestamp2, 0, 4);
                timestamp2 = Bytes.toLong(BinaryUtils.flipBO(tmpTimestamp2, 4));
                if (this.min_captime <= timestamp2 && timestamp2 < this.max_captime && wiredlen2 > 53 && wiredlen2 < 65535 && caplen1 > 0  && caplen2 > 0 && caplen2 <= wiredlen2 && timestamp2 >= timestamp1 && timestamp2 - timestamp1 < endureTime) {
                    return pos;
                }
            } 
            ++pos;
        }
        return pos;
    }

    int readPacket(int packetLen) throws IOException {
        int bufferPosn = 16;
        byte[] tmp_buffer = new byte[packetLen];
        this.bufferLength = this.in.read(tmp_buffer);
        if (this.bufferLength < packetLen) {

        	if(this.bufferLength != -1)
                bufferPosn+=this.bufferLength;
            return bufferPosn;
                
        } else {
            System.arraycopy(tmp_buffer, 0, this.buffer, bufferPosn, this.bufferLength);
        }
        bufferPosn+=this.bufferLength;
        return bufferPosn;
    }

    /*
     * Enabled aggressive block sorting
     * Enabled unnecessary exception pruning
     */
    int readPacketHeader() {
        int headerLength = 0;
        int headerPosn = 0;
        this.pcap_header = new byte[16];
        //byte[] tmp_header = new byte[16];
        BytesWritable capturedLen = new BytesWritable();
        try {
            headerLength = this.in.read(this.pcap_header);
            if (headerLength < 16) {
                if (headerLength == -1) {
                    return 0;
                }
                headerPosn+=headerLength;
                byte[] newheader = new byte[16 - headerLength];
                if ((headerLength = this.in.read(newheader)) < 0) {
                    this.consumed = headerPosn;
                    return -1;
                }
                System.arraycopy(newheader, 0, this.pcap_header, headerPosn, headerLength);
            }
            capturedLen.set(this.pcap_header, 8, 4);
            System.arraycopy(this.pcap_header, 0, this.buffer, 0, 16);
            return Bytes.toInt(BinaryUtils.flipBO(capturedLen.getBytes(), 4));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return Bytes.toInt(BinaryUtils.flipBO(capturedLen.getBytes(), 4));
    }

    public int readFileHeader() {
        try {
            byte[] magic = new byte[4];
            this.bufferLength = this.in.read(this.buffer, 0, 24);
            System.arraycopy(this.buffer, 0, magic, 0, magic.length);
            if (Bytes.toInt(magic) != -725372255) {
                return 0;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return this.bufferLength;
    }

    public int readLine(BytesWritable bytes, int maxLineLength, int maxBytesToConsume) throws IOException {
        bytes.set(new BytesWritable());
        boolean hitEndOfFile = false;
        long bytesConsumed = 0;
        int caplen = this.readPacketHeader();
        if (caplen == 0) {
            bytesConsumed = 0;
        } else if (caplen == -1) {
            bytesConsumed+=(long)this.consumed;
        } else if (caplen > 0 &&  caplen < 65535+14) {
            this.bufferLength = this.readPacket(caplen);
            if (this.bufferLength < caplen + 16) {
                hitEndOfFile = true;
            }
            bytesConsumed+=(long)this.bufferLength;
            if (!hitEndOfFile) {
                bytes.set(this.buffer, 0, caplen + 16);
            }
        }
        return (int)Math.min(bytesConsumed, Integer.MAX_VALUE);
    }

    public int readLine(BytesWritable str, int maxLineLength) throws IOException {
        return this.readLine(str, maxLineLength, Integer.MAX_VALUE);
    }

    public int readLine(BytesWritable str) throws IOException {
        return this.readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }
}

