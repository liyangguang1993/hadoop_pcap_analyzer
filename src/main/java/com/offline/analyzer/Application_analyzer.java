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
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import com.offline.Package_IO.*;
import com.offline.Package_IO.BinaryUtils;
import com.offline.Package_IO.Bytes;

public class Application_analyzer {
		
	public static boolean isFilter = false;
	public JobConf conf;
	private int period;
	private int hour = 60*60*1000;
	private String srcFileName;
	private String dstFileName;
	
	public Application_analyzer(){
		this.conf = new JobConf();
		this.period = 24;
	}
	public Application_analyzer(JobConf conf){
		this.conf = conf;
		this.period = conf.getInt("pcap.record.rate.period", 24);
		this.srcFileName = conf.getStrings("pcap.record.srcDir")[0];
		this.dstFileName = conf.getStrings("pcap.record.dstDir")[0];
	}
	
	public void start(){
        
    	try{
	        Date date = new Date();
    		long time = date.getTime();
    		time =  time-(time+8*hour)%(period*hour);
    		String Date = Long.toString(time);
    		String dstFilename= dstFileName + "state1/"+Date+"/";
    	    Path output_path = new Path(dstFilename);
    	    Path inputDir = new Path(srcFileName);
    	    
    		FileSystem fs = FileSystem.get(conf);
	        JobConf job_state1 = get_state1_JobConf("transport_analyse_job", inputDir, output_path);   
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(job_state1))) {
	          fs.delete(FileOutputFormat.getOutputPath(job_state1), true);
	        }
	        JobClient.runJob(job_state1);  
	       
	        dstFilename= dstFileName + "state2/"+Date+"/";
    	    output_path = new Path(dstFilename);
	        Path job1_output = FileOutputFormat.getOutputPath(job_state1);
	        JobConf job_state2 = get_state2_JobConf("transport_analyse_job2", job1_output, output_path);  
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(job_state2))) {
	          fs.delete(FileOutputFormat.getOutputPath(job_state2), true);
	        }
	        JobClient.runJob(job_state2);
	        
	       /* Path job2_output = FileOutputFormat.getOutputPath(job_state2);
	        
	        dstFilename= outputDir + "state3/"+Date+"/";
    	   output_path = new Path(dstFilename);
	        JobConf job_state3 = get_state3_JobConf("transport_analyse_job3", job2_output, output_path);  
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(job_state3))) {
	          fs.delete(FileOutputFormat.getOutputPath(job_state3), true);
	        }
	        JobClient.runJob(job_state3);*/
	        
        }catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }
	
	private JobConf get_state1_JobConf(String jobName, Path inFilePath, Path outFilePath){//获取第一阶段工作配置

        conf.setJobName(jobName);     
        conf.setNumReduceTasks(1);       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	       
       	conf.setInputFormat(PcapInputFormat.class);          
        conf.setOutputFormat(TextOutputFormat.class);     
        conf.setMapperClass(Map_Stats1.class);
        //conf.setCombinerClass(Reduce_Stats1.class);          
        conf.setReducerClass(Reduce_Stats1.class);    
        MultipleOutputs.addNamedOutput(conf,"pc",TextOutputFormat.class,Text.class,LongWritable.class);  
        MultipleOutputs.addNamedOutput(conf,"bc",TextOutputFormat.class,Text.class,LongWritable.class); 
        MultipleOutputs.addNamedOutput(conf,"flow",TextOutputFormat.class,Text.class,LongWritable.class); 
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}

	private JobConf get_state2_JobConf(String jobName, Path inFilePath, Path outFilePath){//获取第二阶段工作配置
		JobConf conf = new JobConf(Application_analyzer.class);

        conf.setJobName(jobName); 
        conf.setNumReduceTasks(1);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	   
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setMapperClass(Map_Stats2.class);
        conf.setCombinerClass(Reduce_Stats2.class);
        conf.setReducerClass(Reduce_Stats2.class);    

        MultipleOutputs.addNamedOutput(conf,"pc",TextOutputFormat.class,Text.class,LongWritable.class);  
        MultipleOutputs.addNamedOutput(conf,"bc",TextOutputFormat.class,Text.class,LongWritable.class); 
        MultipleOutputs.addNamedOutput(conf,"flow",TextOutputFormat.class,Text.class,LongWritable.class); 
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	} 
	
	@SuppressWarnings("unused")
	private JobConf get_state3_JobConf(String jobName, Path inFilePath, Path outFilePath){//获取第二阶段工作配置
		JobConf conf = new JobConf(Application_analyzer.class);

        conf.setJobName(jobName); 
        conf.setNumReduceTasks(1);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	   
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setMapperClass(Map_Stats3.class);
        conf.setCombinerClass(Reduce_Stats3.class);
        conf.setReducerClass(Reduce_Stats3.class);    
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	} 
	
	public static class Map_Stats1 extends MapReduceBase 
	implements Mapper<LongWritable, BytesWritable, Text, LongWritable>{
		private int interval = 60;		
		private byte[] ip_ver = new byte[1];											//ip协议版本
		private byte[] proto = new byte[1];								//协议类型
		private byte[] timestamp = new byte[4];					//开始时间
		private byte[] caplen = new byte[4];
		private byte[] port = new byte[2];
		private byte[] hlen = new byte[1];
	    private String protocol_type;
		private long Caplen;
		private long Timestamp;
	    private int src_port;
	    private int dst_port;
		private int Optlen;
		private byte[] value_bytes;
		
	    private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		
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
				
				//获取协议类型
				System.arraycopy(value_bytes, PcapRec.POS_IPV4_PROTO, proto, 0 , PcapRec.LEN_IPV4_PROTO);	
				
				if(Bytes.toInt(proto) == PcapRec.ICMP){
					protocol_type = "ICMP";
					
				}else if(Bytes.toInt(proto) == PcapRec.TCP){
					//获取ip头长度，以定位到端口位置
					System.arraycopy(value_bytes, PcapRec.POS_HL_IPV4, hlen, 0, hlen.length);					
					Optlen = (hlen[0] & 0x0f)*4 - 20;
					//获取端口号
					System.arraycopy(value_bytes, PcapRec.POS_IPV4_SP+Optlen, port, 0, port.length);
					src_port = Bytes.toInt(port);
					System.arraycopy(value_bytes, PcapRec.POS_IPV4_DP+Optlen, port, 0, port.length);
					dst_port = Bytes.toInt(port);
					
					protocol_type = get_protocol_type_UDP(src_port, dst_port);
					
				}else if(Bytes.toInt(proto) == PcapRec.UDP){
					//获取ip头长度，以定位到端口位置
					System.arraycopy(value_bytes, PcapRec.POS_HL_IPV4, hlen, 0, hlen.length);					
					Optlen = (hlen[0] & 0x0f)*4 - 20;
					//获取端口号
					System.arraycopy(value_bytes, PcapRec.POS_IPV4_SP+Optlen, port, 0, port.length);
					src_port = Bytes.toInt(port);
					System.arraycopy(value_bytes, PcapRec.POS_IPV4_DP+Optlen, port, 0, port.length);
					dst_port = Bytes.toInt(port);
					
					protocol_type = get_protocol_type_UDP(src_port, dst_port);
					
				}else{
					protocol_type = "UNKNOW";
				}
				
			}else if(ip_ver[0]== PcapRec.IPV6){
				
				//获取协议类型
				System.arraycopy(value_bytes, PcapRec.POS_IPV6_PROTO, proto, 0 , PcapRec.LEN_IPV6_PROTO);	
				
				if(Bytes.toInt(proto) == PcapRec.ICMP){
					protocol_type = "ICMP";
				}else if(Bytes.toInt(proto) == PcapRec.TCP){
					//获取端口号
					System.arraycopy(value_bytes, PcapRec.POS_IPV6_SP+Optlen, port, 0, port.length);
					src_port = Bytes.toInt(port);
					System.arraycopy(value_bytes, PcapRec.POS_IPV6_DP+Optlen, port, 0, port.length);
					dst_port = Bytes.toInt(port);
					
					protocol_type = get_protocol_type_UDP(src_port, dst_port);
					
				}else if(Bytes.toInt(proto) == PcapRec.UDP){
					//获取端口号
					System.arraycopy(value_bytes, PcapRec.POS_IPV6_SP+Optlen, port, 0, port.length);
					src_port = Bytes.toInt(port);
					System.arraycopy(value_bytes, PcapRec.POS_IPV6_DP+Optlen, port, 0, port.length);
					dst_port = Bytes.toInt(port);
					
					protocol_type = get_protocol_type_UDP(src_port, dst_port);
					
				}else{
					protocol_type = "UNKNOW";
				}
			}else{
				protocol_type = "UNKNOW";
			}
			text.clear();
			text.set(Long.toString(Timestamp) + "\t"+protocol_type);
	        longwrite.set(Caplen);
	        output.collect(text, longwrite);	
	        
	    }//map
	}//Map_States1
	
	public static class Reduce_Stats1 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {	
	
		private MultipleOutputs multipleoutputs;
		private  long sum = 0;
		private long count = 0;
		private String temp;
		@Override
		public void configure(JobConf conf){
			multipleoutputs = new MultipleOutputs(conf);
		}
		
	    public void reduce(Text key, Iterator<LongWritable> value,
	                    OutputCollector<Text, LongWritable> output, Reporter reporter)
	                    throws IOException {
	      sum = 0;
	      count = 0;
	       while(value.hasNext()){
	        	sum += value.next().get();
	        	count ++;
	        }
	        temp = key.toString();
	        key.clear();
	        key.set("bc\t"+temp);
	        output.collect(key, new LongWritable(sum));
	        key.clear();
	        key.set("pc\t"+temp);
	        output.collect(key, new LongWritable(count));
	  
	    }
	    
	    @Override
	    public void close() throws IOException{
	    	multipleoutputs.close();
	    }
	}


	public static class Map_Stats2 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, LongWritable>{
		String[] substring;
		String[] s;
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			
			substring = value.toString().split("\t");
			output.collect(new Text(substring[0]+"\t"+substring[2]), new LongWritable(Long.parseLong(substring[3])));
			
			
		}
		
	}
	
    public static class Reduce_Stats2 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {
    	private long sum;
    	private MultipleOutputs multipleoutputs;
    	@Override
		public void configure(JobConf conf){
			multipleoutputs = new MultipleOutputs(conf);
		}

		public void reduce(Text key, Iterator<LongWritable> value,
	                    OutputCollector<Text, LongWritable> output, Reporter reporter)
	                    throws IOException {
	
	      sum = 0;
	      //substring = key.toString().split("\t");
	       while(value.hasNext()) 		 				
	    	   sum += value.next().get();
	       output.collect(key, new LongWritable(sum));        
	       /*substring = key.toString().split("\t");
	    	if(substring[0].toString().equals("bc")){
	    		collector = multipleoutputs.getCollector("bc", reporter);
	    	}else if(substring[0].toString().equals("pc")){
	    		collector = multipleoutputs.getCollector("pc", reporter);
	    	}else if(substring[0].toString().equals("flow")){
	    		collector = multipleoutputs.getCollector("flow", reporter);
	    	}
	    	collector.collect(key, new LongWritable(sum));*/
	       //output.collect(key, new LongWritable(sum));        
	    }
	    @Override
	    public void close() throws IOException{
	    	multipleoutputs.close();
	    }
    }
	
	public static class Map_Stats3 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, LongWritable>{
		String[] substring;
		String[] s;
		public void map
				(LongWritable key, Text value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			
			substring = value.toString().split("\t");
			if(substring[0].toString().equals("flow")){
				output.collect(new Text(substring[2]), new LongWritable(Long.parseLong(substring[3].trim())));
			}
			
		}
		
	}
	
    public static class Reduce_Stats3 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {
    	private long sum;
    	String[] substring;
	    public void reduce(Text key, Iterator<LongWritable> value,
	                    OutputCollector<Text, LongWritable> output, Reporter reporter)
	                    throws IOException {
	
	      sum = 0;
	      substring = key.toString().split("\t");
	       while(value.hasNext()) 		 				
	    	   sum += value.next().get();
	       output.collect(key, new LongWritable(sum));        
	    }
    }
	
	
	/*　dns 53/tcp或/udp
	　　smtp 25/tcp
	　　POP3 110/tcp
	　　HTTP 80/tcp
	　　https 443/udp
	　　telnet 23/tcp
	　　ftp 20/21/tcp
	　　tftp 69/udp
	　　imap 143/tcp
	　　snmp 161/udp
	　　snmptrap 162/udp:*/
	public static String get_protocol_type_TCP(int port1, int port2){
		switch(port1){
		case 53:
			return "DNS";
		/*case 25:
			return "SMTP";
		case 110:
			return "POP3";*/
		case 80:
			return "HTTP";
		case 23:
			return "TELNET";/*
		case 20:
			return "FTP";
		case 21:
			return "FTP";
		case 143:
			return "IMAP";
		case 67:
			return "DHCP";*/
		case 135:
			return "RPC";
		default:
			;
		}
		switch(port2){
		case 53:
			return "DNS";
		/*case 25:
			return "SMTP";
		case 110:
			return "POP3";*/
		case 80:
			return "HTTP";
		case 23:
			return "TELNET";/*
		case 20:
			return "FTP";
		case 21:
			return "FTP";
		case 143:
			return "IMAP";
		case 67:
			return "DHCP";*/
		case 135:
			return "RPC";
		default:
			;
		}
		return "UNKNOW";
	}
	
	public static String get_protocol_type_UDP(int port1, int port2){
		switch(port1){
		case 53:
			return "DNS";
		/*case 443:
			return "HTTPS";
		case 69:
			return "TFTP";*/
		case 161:
			return "SNMP";
		/*case 162:
			return "SNMPTRAP";*/
		case 1900:
			return "SSDP";
		case 137:
			return "NBNS";
		//case 5355:
		//	return "LLMNR";
		default:
			;
		}
		switch(port2){
		case 53:
			return "DNS";
		/*case 443:
			return "HTTPS";
		case 69:
			return "TFTP";*/
		case 161:
			return "SNMP";
		/*case 162:
			return "SNMPTRAP";*/
		case 1900:
			return "SSDP";
		case 137:
			return "NBNS";
		//case 5355:
		//	return "LLMNR";
		default:
			;
		}
		return "UNKNOW";
	}
}
