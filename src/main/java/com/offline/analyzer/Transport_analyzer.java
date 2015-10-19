package com.offline.analyzer;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.offline.Package_IO.PcapRec;

import com.offline.Package_IO.BinaryUtils;
import com.offline.Package_IO.Bytes;
import com.offline.Package_IO.PcapInputFormat;

public class Transport_analyzer {
		
	public static boolean isFilter = false;
	public JobConf conf;
	private int period;
	private int hour = 60*60*1000;
	private String srcFileName;
	private String dstFileName;
	private int reducer_num;
	
	public Transport_analyzer(){
		this.conf = new JobConf();
		this.period = 24;
	}
	
	public Transport_analyzer(JobConf conf){
		this.conf = conf;
		this.period = conf.getInt("pcap.record.rate.period", 0);
		this.srcFileName = conf.getStrings("pcap.record.srcDir")[0];
		this.dstFileName = conf.getStrings("pcap.record.dstDir")[0];
		this.reducer_num = conf.getInt("pcap.record.sort.topN", 1);
	}
	
    public void start(){
        
    	try{
    		Date date = new Date();
    		long time = date.getTime();
    		time =  time-(time+8*hour)%(period*hour);
    		String Date = Long.toString(time);
    		String dstFilename= dstFileName + "period/"+Date+"/";
    	    Path output_path = new Path(dstFilename);
    	    Path inputDir = new Path(srcFileName);
    	    
	        FileSystem fs = FileSystem.get(conf);
	        JobConf job_state1 = get_transport_analyse_JobConf("transport_analyse_job", inputDir, output_path);   
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(job_state1))) {
	          fs.delete(FileOutputFormat.getOutputPath(job_state1), true);
	        }
	        JobClient.runJob(job_state1);  
	        
	        dstFilename= dstFileName + "total/"+Date+"/";
    	    output_path = new Path(dstFilename);
	        Path job1_output = FileOutputFormat.getOutputPath(job_state1);
	        JobConf job_state2 = get_total_count_JobConf("transport_analyse_job2", job1_output, output_path);  
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(job_state2))) {
	          fs.delete(FileOutputFormat.getOutputPath(job_state2), true);
	        }
	        JobClient.runJob(job_state2);
	       
        }catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }

	private JobConf get_transport_analyse_JobConf(String jobName, Path inFilePath, Path outFilePath){//获取第一阶段工作配置
		
        conf.setJobName(jobName);     
        conf.setNumReduceTasks(reducer_num);       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	       
       	conf.setInputFormat(PcapInputFormat.class);          
        conf.setOutputFormat(TextOutputFormat.class);     
        conf.setMapperClass(Map_Stats1.class);
        //conf.setCombinerClass(Reduce_Stats1.class);          
        conf.setReducerClass(Reduce_Stats1.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}

	private JobConf get_total_count_JobConf(String jobName, Path inFilePath, Path outFilePath){//获取第二阶段工作配置
		
		JobConf conf = new JobConf(Transport_analyzer.class);
 
        conf.setJobName(jobName); 
        conf.setNumReduceTasks(1);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	   
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setMapperClass(Map_Stats2.class);
        conf.setCombinerClass(Reduce_Stats2.class);
        conf.setReducerClass(Reduce_Stats2.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	} 
	
	public static class Map_Stats1 extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, Text, LongWritable>{
	    private int interval = 60;
		byte[] ip_ver = new byte[1];											//ip协议版本
		byte[] proto = new byte[1];								//协议类型
		byte[] timestamp = new byte[4];					//开始时间
		byte[] caplen = new byte[4];
		long Caplen;
		long Timestamp;
		byte[] value_bytes;
	    private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		int i=0;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 0);			
		}	
		
		public void map		
				(LongWritable key, BytesWritable value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {		

			value_bytes = value.getBytes();
			if(value_bytes.length < 42) return;//若不够长则不分析
			
			//获取时间戳
			System.arraycopy(value_bytes, PcapRec.POS_TSTAMP, timestamp, 0 , PcapRec.LEN_TSTAMP);	
			Timestamp = Bytes.toLong(BinaryUtils.flipBO(timestamp,4));
			Timestamp = Timestamp - (Timestamp%interval);

			//获取数据包长度
			System.arraycopy(value_bytes, PcapRec.POS_CAPLEN, caplen, 0 , PcapRec.LEN_CAPLEN);	
			Caplen = Bytes.toInt(BinaryUtils.flipBO(caplen, 4));
			
			//获取ip-version
			System.arraycopy(value_bytes, PcapRec.POS_IP_VER, ip_ver, 0, PcapRec.LEN_IP_VER);
			 ip_ver[0] = (byte) (ip_ver[0]&0xf0);	
			if(ip_ver[0]== PcapRec.IPV4){
				/*text.clear();
				text.set("IPV4");
				longwrite.set(1);
				output.collect(text, longwrite);	*/
				//获取协议类型
				System.arraycopy(value_bytes, PcapRec.POS_IPV4_PROTO, proto, 0 , PcapRec.LEN_IPV4_PROTO);	
				text.clear();
				if(Bytes.toInt(proto) == PcapRec.ICMP){
            		text.set(Long.toString(Timestamp)+"\tICMP");
				}else if(Bytes.toInt(proto) == PcapRec.TCP){
					text.set(Long.toString(Timestamp)+"\tTCP");
				}else if(Bytes.toInt(proto) == PcapRec.UDP){
					text.set(Long.toString(Timestamp)+"\tUDP");
				}else{
					text.set(Long.toString(Timestamp)+"\tOTHER");
				}
				longwrite.set(Caplen);
        		output.collect(text, longwrite);	
        		
			}else if(ip_ver[0] == PcapRec.IPV6){
				/*text.clear();
				text.set("IPV6");
				longwrite.set(1);
				output.collect(text, longwrite);	*/
				
				System.arraycopy(value_bytes, PcapRec.POS_IPV6_PROTO, proto, 0 , PcapRec.LEN_IPV6_PROTO);	
				text.clear();
				if(Bytes.toInt(proto) == PcapRec.ICMPV6){
            		text.set(Long.toString(Timestamp)+"\tICMP");
				}else if(Bytes.toInt(proto) == PcapRec.TCP){
					text.set(Long.toString(Timestamp)+"\tTCP");
				}else if(Bytes.toInt(proto) == PcapRec.UDP){
					text.set(Long.toString(Timestamp)+"\tUDP");
				}else{
					text.set(Long.toString(Timestamp)+"\tOTHER");
				}
				longwrite.set(Caplen);
        		output.collect(text, longwrite);	
        		
			}else{
				text.clear();
				//text.set("NON_IP");
				text.set(Long.toString(Timestamp)+"\tNON_IP");
				longwrite.set(Caplen);
        		output.collect(text, longwrite);		
			}
			
		}//Map_stats1
	}
	
	public static class Reduce_Stats1 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {	

        private long sum = 0;
        private long count = 0;
        private String temp;
        
	    public void reduce(Text key, Iterator<LongWritable> value,
	                    OutputCollector<Text, LongWritable> output, Reporter reporter)
	                    throws IOException {
	    	sum = 0;
	    	count = 0;
	        while(value.hasNext()){
	        	sum += value.next().get();
	        	count++;
	        }
	        temp = key.toString();
	        key.clear();
	        key.set("bc\t"+temp);
	        output.collect(key, new LongWritable(sum));
	        key.clear();
	        key.set("pc\t"+temp);
	        output.collect(key, new LongWritable(count));
	    }
	}
	
	public static class Map_Stats2 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, LongWritable>{
		
		public void map
				(LongWritable key, Text value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			String[] substring = value.toString().split("\t");
			if(substring[0].equals("bc")){
				output.collect(new Text("bc\t"+substring[2]), new LongWritable(Long.parseLong(substring[substring.length-1].trim())));
			}else{
				output.collect(new Text("pc\t"+substring[2]), new LongWritable(Long.parseLong(substring[substring.length-1].trim())));
			}
			//output.collect(new Text(substring[1]), new LongWritable(Long.parseLong(substring[2])));
		}
		
	}
	
    public static class Reduce_Stats2 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {
	    public void reduce(Text key, Iterator<LongWritable> value,
	                    OutputCollector<Text, LongWritable> output, Reporter reporter)
	                    throws IOException {
	
	       long sum = 0;
	       while(value.hasNext()) 		 				
	    	   sum += value.next().get();
	       output.collect(key, new LongWritable(sum));        
	       
	    }
    }

}
