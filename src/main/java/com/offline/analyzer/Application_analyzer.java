package com.offline.analyzer;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
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

import com.offline.Package_IO.CombinePcapInputFormat;
import com.offline.Package_IO.Packet;

public class Application_analyzer {
		
	public static boolean isFilter = false;
	public JobConf conf;
	private int period;
	private int hour = 60*60*1000;
	public Application_analyzer(){
		this.conf = new JobConf();
		this.period = 24;
	}
	public Application_analyzer(JobConf conf){
		this.conf = conf;
		this.period = conf.getInt("pcap.record.rate.period", 0);
	}
	
	public void start(Path inputDir, String outputDir){
        
    	try{
	        Date date = new Date();
    		long time = date.getTime();
    		time =  time-(time+8*hour)%(period*hour);
    		String Date = Long.toString(time);
    		String dstFilename= outputDir + "state1/"+Date+"/";
    	    Path output_path = new Path(dstFilename);
    	    
    		FileSystem fs = FileSystem.get(conf);
	        JobConf job_state1 = get_state1_JobConf("transport_analyse_job", inputDir, output_path);   
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(job_state1))) {
	          fs.delete(FileOutputFormat.getOutputPath(job_state1), true);
	        }
	        JobClient.runJob(job_state1);  
	       
	        dstFilename= outputDir + "state2/"+Date+"/";
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
       	conf.setInputFormat(CombinePcapInputFormat.class);          
        conf.setOutputFormat(TextOutputFormat.class);     
        conf.setMapperClass(Map_Stats1.class);
        conf.setCombinerClass(Reduce_Stats1.class);          
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
	implements Mapper<LongWritable, ObjectWritable, Text, LongWritable>{
		private int interval = 60;		
	    private Object object;
	    private Packet packet;
	    private int ip_version;
	    private String protocol;
	    private long timestamp;
	    private String src_ip;
	    private String dst_ip;
	    private int src_port;
	    private int dst_port;
	    private int pc;
	    private int bc;
	    private String protocol_type;
	    private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 0);			
		}	
		
		public void map		
				(LongWritable key, ObjectWritable value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			packet = (Packet)value.get();
			
	        if (packet != null) {
	        	object = packet.get(Packet.IP_VERSION);
	        	
	            if (object != null) {
	            	ip_version = (Integer)object;
	            	timestamp = (Long)packet.get(Packet.TIMESTAMP);
	            	timestamp = timestamp - timestamp%interval;
	            	
	            	if(ip_version == 4 || ip_version == 6){
	            		
	            		protocol = (String) packet.get(Packet.PROTOCOL);
	            		bc = (Integer) packet.get(Packet.LEN);
	            		pc = 1;
	            		object = packet.get(Packet.SRC_PORT);
	            		if(object != null)		src_port = (Integer)object;
	            		else	src_port = -1;
	            		object = packet.get(Packet.DST_PORT);
	            		if(object != null)		src_port = (Integer)object;
	            		else	src_port = -1;
	            		if(protocol != null)
	            		if(protocol.equals("UDP"))
	            			protocol_type = get_protocol_type_UDP(src_port, dst_port);
	            		else if(protocol.equals("TCP"))
	            			protocol_type = get_protocol_type_TCP(src_port, dst_port);
	            		else
	            			protocol_type = "UNKNOW";
	            		
	            		text.clear();
	            		text.set(/*"bc\t"+*/Long.toString(timestamp) + "\t"+protocol_type);
	            		longwrite.set(bc);
	            		output.collect(text, longwrite);	
	            		/*text.clear();
	            		text.set("pc\t"+Long.toString(timestamp) + "\t"+protocol_type);
	            		longwrite.set(pc);
	            		output.collect(text, longwrite);	
	            		
	            		src_ip = (String)packet.get(Packet.SRC);
	            		dst_ip = (String)packet.get(Packet.DST);
	            		text.clear();
						text.set("flow\t"+Long.toString(timestamp) + "\t" + src_ip  + "\t"+ dst_ip+"\t"+protocol_type);
						longwrite.set(1);
						output.collect(text, longwrite);	*/
	            	}else{
	            		//TODO
	            		//如何处理非ipv4或ipv6的数据包？
	            	}
	            }else{//ip_version == null
	            	/*timestamp = (Long)packet.get(Packet.TIMESTAMP);
	            	timestamp = timestamp - timestamp%interval;
	            	text.clear();
	        		text.set("pc\t"+Long.toString(timestamp)+"\tUNKNOW");
	        		longwrite.set(1);
	        		output.collect(text, longwrite);	*/
	            }//if(IP)
	        }
	    }//map
	}//Map_States1
	
	public static class Reduce_Stats1 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {	
	
		private MultipleOutputs multipleoutputs;
		private  long sum = 0;
		private String[] k;
    	private OutputCollector<Text, LongWritable> collector ;
		@Override
		public void configure(JobConf conf){
			multipleoutputs = new MultipleOutputs(conf);
		}
		
	    @SuppressWarnings("unchecked")
		public void reduce(Text key, Iterator<LongWritable> value,
	                    OutputCollector<Text, LongWritable> output, Reporter reporter)
	                    throws IOException {
	       sum = 0;
	      
	        while(value.hasNext()){
	        	sum += value.next().get();
	        }
	        output.collect(key, new LongWritable(sum)); 
	        /*k = key.toString().split("\t");
	    	if(k[0].equals("bc")){
	    		collector = multipleoutputs.getCollector("bc", reporter);
	    		collector.collect(key, new LongWritable(sum));
	    	}else if(k[0].equals("pc")){
	    		collector = multipleoutputs.getCollector("pc", reporter);
	    		collector.collect(key, new LongWritable(sum));
	    	}else if(k[0].equals("flow")){
	    		collector = multipleoutputs.getCollector("flow", reporter);
	    		collector.collect(new Text(k[0].toString() +"\t"+ k[1].toString() + "\t"+ k[4].toString()), new LongWritable(1));
	    	}*/
	    	
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
			output.collect(new Text(substring[1]), new LongWritable(Long.parseLong(substring[2].trim())));
			/*if(substring[0].equals("flow")){
				output.collect(new Text(substring[0]+"\t"+substring[1]+"\t"+substring[2]), new LongWritable(Long.parseLong(substring[3].trim())));
			}else{
				output.collect(new Text(substring[0]+"\t"+substring[2]), new LongWritable(Long.parseLong(substring[3].trim())));
			}*/
			
			
		}
		
	}
	
    public static class Reduce_Stats2 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {
    	private long sum;
    	private String[] substring;
    	private MultipleOutputs multipleoutputs;
    	private OutputCollector<Text, LongWritable> collector ;
    	@Override
		public void configure(JobConf conf){
			multipleoutputs = new MultipleOutputs(conf);
		}
	    @SuppressWarnings("unchecked")
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
