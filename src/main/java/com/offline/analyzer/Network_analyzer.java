package com.offline.analyzer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
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

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;

import com.offline.Package_IO.CombinePcapInputFormat;
import com.offline.Package_IO.Packet;

public class Network_analyzer {
		
	public static boolean isFilter = false;
	public JobConf conf;
	public long period;
	public int hour=60*60*1000;
	
	public Network_analyzer(){
		this.conf = new JobConf();
		this.period = 24;
	}
	
	public Network_analyzer(JobConf conf){
		this.conf = conf;
		this.period = conf.getInt("pcap.record.rate.period", 0);
	}
	
	/*
	 * 文件
	 * @param
	 * @param
	 * @return  
	 */
    public void start(Path inputDir, String outputDir, long cap_start, long cap_end){// throws IOException {//分析程序入口
        
    	System.out.println("start1");
    	try{
    		Date date = new Date();
    		long time = date.getTime();
    		time =  time-(time+8*hour)%(period*hour);
    		String Date = Long.toString(time);

    		String dstFilename= outputDir + "country_period/"+Date+"/";
    	    Path output_path = new Path(dstFilename);

	        FileSystem fs = FileSystem.get(conf);
	        JobConf State1 = get_state1_JobConf("state1", output_path, inputDir);   
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(State1))) {
	          fs.delete(FileOutputFormat.getOutputPath(State1), true);
	        }
	        JobClient.runJob(State1);  

	        dstFilename= outputDir + "country_total/"+Date+"/";
	        output_path = new Path(dstFilename);
	        Path state2_inputPath = FileOutputFormat.getOutputPath(State1);
	        JobConf total_count = get_state2_JobConf("state2", state2_inputPath, output_path);  
	        
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(total_count))) {
	          fs.delete(FileOutputFormat.getOutputPath(total_count), true);
	        }
	        JobClient.runJob(total_count);
	        
        }catch (IOException e) {
    		e.printStackTrace();
    	}
    }

	private JobConf get_state1_JobConf(String jobName, Path outFilePath, Path inFilePath){//获取第一阶段工作配置
		JobConf conf = new JobConf(this.conf, Network_analyzer.class);
		
        conf.setJobName(jobName);     
        conf.setNumReduceTasks(1);       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);	       
       	conf.setInputFormat(CombinePcapInputFormat.class);          
        conf.setOutputFormat(TextOutputFormat.class);     
        conf.setMapperClass(Map_Stats1.class);
        conf.setCombinerClass(Reduce_Stats1.class);          
        conf.setReducerClass(Reduce_Stats1.class);    

        MultipleOutputs.addNamedOutput(conf,"pcsrc",TextOutputFormat.class,Text.class,LongWritable.class);  
        MultipleOutputs.addNamedOutput(conf,"pcdst",TextOutputFormat.class,Text.class,LongWritable.class); 
        MultipleOutputs.addNamedOutput(conf,"bcsrc",TextOutputFormat.class,Text.class,LongWritable.class);  
        MultipleOutputs.addNamedOutput(conf,"bcdst",TextOutputFormat.class,Text.class,LongWritable.class); 
        
        FileInputFormat.setInputPaths(conf, inFilePath);
        FileOutputFormat.setOutputPath(conf, outFilePath);
        
        return conf;
	}
	//总数据统计阶段
	private JobConf get_state2_JobConf(String jobName, Path inFilePath, Path outFilePath){//获取第二阶段工作配置
		JobConf conf = new JobConf(this.conf, Network_analyzer.class);
 
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
		private int interval = 60;		
	    private Object object;
	    private Packet packet;
	    private int ip_version;
	    private long timestamp;
	    private String src_ip;
	    private String dst_ip;
	    private int bc;
	    private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		private String[] dbDir;
		private InetAddress ipAddress;
		private File database;
		private DatabaseReader reader;
		private CountryResponse response;
		private Country country;
		private String country_name;
		private boolean exist;
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 0);		
			dbDir = conf.getStrings("pcap.record.dbDir");
			database = new File(dbDir[0]+"GeoLite2-Country.mmdb");
			 try {
				reader = new DatabaseReader.Builder(database).build();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
	            		
	            		bc = (Integer) packet.get(Packet.LEN);
	            		src_ip = (String)packet.get(Packet.SRC);
	            		
	            		exist = true;
	            		ipAddress = InetAddress.getByName(src_ip);
	            		try {
							response = reader.country(ipAddress);
						} catch (GeoIp2Exception e) {
							exist = false;
						}
	            		
	            		if(exist == true){
	            			country = response.getCountry();
		            		country_name = get_country(country.getName());
		            		
		            		text.clear();
		            		text.set("bc\tsrc\t" + Long.toString(timestamp) + "\t" + country_name);
							longwrite.set(bc);
							output.collect(text, longwrite);	
							text.clear();
		            		text.set("pc\tsrc\t" + Long.toString(timestamp) + "\t" + country_name);
							longwrite.set(1);
							output.collect(text, longwrite);	
							
		            		dst_ip = (String)packet.get(Packet.DST);
	            		}
	            		
	            		exist = true;
	            		ipAddress = InetAddress.getByName(dst_ip);
	            		try {
							response = reader.country(ipAddress);
						} catch (GeoIp2Exception e) {
							exist = false;
						}
	            		if(exist == true){
	            			country = response.getCountry();
		            		country_name = get_country(country.getName());
		       
		            		text.clear();
							text.set("bc\tdst\t"+Long.toString(timestamp) + "\t" + country_name);
							longwrite.set(bc);
							output.collect(text, longwrite);	
							text.clear();
							text.set("pc\tdst\t"+Long.toString(timestamp) + "\t" + country_name);
							longwrite.set(1);
							output.collect(text, longwrite);	
	            		}
	            		
	            	}else{
	            		//TODO
	            		//如何处理非ipv4或ipv6的数据包？
	            	}
	            }else{//object != null
	            	//TODO
	            	//IP_version = null
	            }//if(IP)
	        }
		}
	}
	
	public static class Reduce_Stats1 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {	
		private String[] s;
		private long sum;
		private MultipleOutputs multipleoutputs;
    	private OutputCollector<Text, LongWritable> collector ;
		private LongWritable longwrite = new LongWritable();
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
	        
	        s = key.toString().split("\t", 3);
	        if(s[0].equals("pc") && s[1].equals("src")){
	        	collector = multipleoutputs.getCollector("pcsrc", reporter);
	        }else if(s[0].equals("bc") && s[1].equals("src")){
	        	collector = multipleoutputs.getCollector("bcsrc", reporter);
	        }else if(s[0].equals("pc") && s[1].equals("dst")){
	        	collector = multipleoutputs.getCollector("pcdst", reporter);
	        }else{
	        	collector = multipleoutputs.getCollector("bcdst", reporter);
	        }
	        longwrite.set(sum);
	        collector.collect(key, longwrite);
	    }

	    @Override
	    public void close() throws IOException{
	    	multipleoutputs.close();
	    }
	}
	
	public static class Map_Stats2 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, LongWritable>{
		private String[] substring;
		private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		public void map
				(LongWritable key, Text value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			
			substring = value.toString().split("\t");
			if(substring[0].equals("pc") && substring[1].equals("src")){
				text.clear();
				text.set("pc_src\t"+substring[3]);
				longwrite.set(Long.parseLong(substring[4].trim()));
				output.collect(text, longwrite);
			}else if(substring[0].equals("bc") && substring[1].equals("src")){
				text.clear();
				text.set("bc_src\t"+substring[3]);
				longwrite.set(Long.parseLong(substring[4].trim()));
				output.collect(text, longwrite);
			}else if(substring[0].equals("pc") && substring[1].equals("dst")){
				text.clear();
				text.set("pc_dst\t"+substring[3]);
				longwrite.set(Long.parseLong(substring[4].trim()));
				output.collect(text, longwrite);
			}else if(substring[0].equals("bc") && substring[1].equals("dst")){
				text.clear();
				text.set("bc_dst\t"+substring[3]);
				longwrite.set(Long.parseLong(substring[4].trim()));
				output.collect(text, longwrite);
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
	
    public static String get_country(String s){
    	if(s != null)
	    	if(s.equals("China"))
	    		return "CHINA";
	    	else if(s.equals("Russia"))
	    		return "RUSSIA";
	    	else if(s.equals("Canada"))
	    		return "CANADA";
	    	else if(s.equals("India"))
	    		return "INDIA";
	    	else if(s.equals("Germany"))
	    		return "GERMANY";
	    	else if(s.equals("Japan"))
	    		return "JAPAN";
	    	else if(s.equals("United States"))
	    		return "US";
	    	else if(s.equals("United Kingdom"))
	    		return "UK";
	    	else if(s.equals("France"))
	    		return "FRANCE";
	    	else
	    		return "OTHERS";
    	else
    		return "OTHERS";
    }
}
