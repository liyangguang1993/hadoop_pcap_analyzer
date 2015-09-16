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

import com.offline.Package_IO.CombinePcapInputFormat;
import com.offline.Package_IO.Packet;

public class Transport_analyzer {
		
	public static boolean isFilter = false;
	public JobConf conf;
	private int period;
	private int hour = 60*60*1000;
	public Transport_analyzer(){
		this.conf = new JobConf();
		this.period = 24;
	}
	
	public Transport_analyzer(JobConf conf){
		this.conf = conf;
		this.period = conf.getInt("pcap.record.rate.period", 0);
	}
	
    public void start(Path inputDir, String outputDir){
        
    	try{
    		Date date = new Date();
    		long time = date.getTime();
    		time =  time-(time+8*hour)%(period*hour);
    		String Date = Long.toString(time);
    		String dstFilename= outputDir + "period/"+Date+"/";
    	    Path output_path = new Path(dstFilename);
    	    
	        FileSystem fs = FileSystem.get(conf);
	        JobConf job_state1 = get_transport_analyse_JobConf("transport_analyse_job", inputDir, output_path);   
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(job_state1))) {
	          fs.delete(FileOutputFormat.getOutputPath(job_state1), true);
	        }
	        JobClient.runJob(job_state1);  
	        
	        dstFilename= outputDir + "total/"+Date+"/";
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
        conf.setNumReduceTasks(1);       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	       
       	conf.setInputFormat(CombinePcapInputFormat.class);          
        conf.setOutputFormat(TextOutputFormat.class);     
        conf.setMapperClass(Map_Stats1.class);
        conf.setCombinerClass(Reduce_Stats1.class);          
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
	implements Mapper<LongWritable, ObjectWritable, Text, LongWritable>{
		private Object object;
	    private Packet packet;
	    private int ip_version;
	    private long timestamp;
	    private int bc;
	    private int interval = 60;
	    private String protocol;
	    private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		int i=0;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 0);			
		}	
		
		public void map		
				(LongWritable key, ObjectWritable value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {		
			
			packet = (Packet)value.get();
	        if (packet != null) {System.out.println(i++);
	        	object = packet.get(Packet.IP_VERSION);
	        	
	            if (object != null) {
	            	ip_version = (Integer)object;
	            	timestamp = (Long)packet.get(Packet.TIMESTAMP);
	            	timestamp = timestamp - timestamp%interval;
	            	
	            	if(ip_version == 4 || ip_version == 6){
	            		protocol = (String)packet.get(Packet.PROTOCOL);
	            		bc = (Integer) packet.get(Packet.LEN);
	            		if(protocol == null){
	            			text.clear();
		            		text.set("pc\t"+Long.toString(timestamp)+"\tUNKNOW");
		            		longwrite.set(1);
		            		output.collect(text, longwrite);	
		            		text.clear();
		            		text.set("bc\t"+Long.toString(timestamp)+"\tUNKNOW");
		            		longwrite.set(bc);
		            		output.collect(text, longwrite);	
	            		}else
	            		if(protocol.equals("TCP")){
	            			text.clear();
		            		text.set("pc\t"+Long.toString(timestamp)+"\tTCP");
		            		longwrite.set(1);
		            		output.collect(text, longwrite);	
	            			text.clear();
		            		text.set("bc\t"+Long.toString(timestamp)+"\tTCP");
		            		longwrite.set(bc);
		            		output.collect(text, longwrite);	
	            		}else if(protocol.equals("UDP")){
	            			text.clear();
		            		text.set("pc\t"+Long.toString(timestamp)+"\tUDP");
		            		longwrite.set(1);
		            		output.collect(text, longwrite);	
	            			text.clear();
		            		text.set("bc\t"+Long.toString(timestamp)+"\tUDP");
		            		longwrite.set(bc);
		            		output.collect(text, longwrite);	
	            		}else if(protocol.equals("ICMP")){
	            			text.clear();
		            		text.set("pc\t"+Long.toString(timestamp)+"\tICMP");
		            		longwrite.set(1);
		            		output.collect(text, longwrite);	
	            			text.clear();
		            		text.set("bc\t"+Long.toString(timestamp)+"\tICMP");
		            		longwrite.set(bc);
		            		output.collect(text, longwrite);	
	            		}else{
	            			text.clear();
		            		text.set("pc\t"+Long.toString(timestamp)+"\tUNKNOW");
		            		longwrite.set(1);
		            		output.collect(text, longwrite);	
	            			text.clear();
		            		text.set("bc\t"+Long.toString(timestamp)+"\tUNKNOW");
		            		longwrite.set(bc);
		            		output.collect(text, longwrite);	
	            		}
	            	}else{
	            		text.clear();
	            		text.set("pc\t"+Long.toString(timestamp)+"\tUNKNOW");
	            		longwrite.set(1);
	            		output.collect(text, longwrite);	
	            	}
	            }else{
	            	timestamp = (Long)packet.get(Packet.TIMESTAMP);
	            	timestamp = timestamp - timestamp%interval;
	            	text.clear();
            		text.set("pc\t"+Long.toString(timestamp)+"\tUNKNOW");
            		longwrite.set(1);
            		output.collect(text, longwrite);	
	            }//if(IP)
	        }//if(package)
		}//Map_stats1
	}
	
	public static class Reduce_Stats1 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {	
	
	    public void reduce(Text key, Iterator<LongWritable> value,
	                    OutputCollector<Text, LongWritable> output, Reporter reporter)
	                    throws IOException {
	        long sum = 0;
	      
	        while(value.hasNext()){
	        	sum += value.next().get();
	        }
	        output.collect(key, new LongWritable(sum));
	    }
	}
	
	public static class Map_Stats2 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, LongWritable>{
		
		public void map
				(LongWritable key, Text value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			String[] substring = value.toString().split("\t");
			if(substring[0].equals("bc")){
				output.collect(new Text("total_bc:"), new LongWritable(Long.parseLong(substring[substring.length-1].trim())));
			}else{
				output.collect(new Text("total_pc:"), new LongWritable(Long.parseLong(substring[substring.length-1].trim())));
			}
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
