package com.offline.analyzer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
import com.offline.Package_IO.BinaryUtils;
import com.offline.Package_IO.Bytes;
import com.offline.Package_IO.PcapRec;

import com.offline.Package_IO.CommonData;
import com.offline.Package_IO.PcapInputFormat;

public class Network_analyzer {
		
	public static boolean isFilter = false;
	public JobConf conf;
	public long period;
	public int hour=60*60*1000;
	private String srcFileName;
	private String dstFileName;
	private long cap_start;
	private long cap_end;
	public Network_analyzer(){
		this.conf = new JobConf();
		this.period = 24;
	}
	
	public Network_analyzer(JobConf conf){
		this.conf = conf;
		this.period = conf.getInt("pcap.record.rate.period", 24);
		this.srcFileName = conf.getStrings("pcap.record.srcDir")[0];
		this.dstFileName = conf.getStrings("pcap.record.dstDir")[0];
	}
	
	/*
	 * 文件
	 * @param
	 * @param
	 * @return  
	 */
    public void start(){// throws IOException {//分析程序入口
        
    	System.out.println("start");
    	try{
    		Date date = new Date();
    		long time = date.getTime();
    		time =  time-(time+8*hour)%(period*hour);
    		String Date = Long.toString(time);

    		String dstFilename= this.dstFileName + "country_period/"+Date+"/";
    	    Path output_path = new Path(dstFilename);
    	    Path inputDir = new Path(this.srcFileName);
    	    
	        FileSystem fs = FileSystem.get(conf);
	        JobConf State1 = get_state1_JobConf("state1", output_path, inputDir);   
	        // delete any output that might exist from a previous run of this job
	        if (fs.exists(FileOutputFormat.getOutputPath(State1))) {
	          fs.delete(FileOutputFormat.getOutputPath(State1), true);
	        }
	        JobClient.runJob(State1);  

	        dstFilename= this.dstFileName + "country_total/"+Date+"/";
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
       	conf.setInputFormat(PcapInputFormat.class);          
        conf.setOutputFormat(TextOutputFormat.class);     
        conf.setMapperClass(Map_Stats1.class);
        //conf.setCombinerClass(Reduce_Stats1.class);          
        conf.setReducerClass(Reduce_Stats1.class);   
        
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
	implements Mapper<LongWritable, BytesWritable, Text, LongWritable>{
	    private String src_ip;
	    private String dst_ip;
		private String dbDir;
		private InetAddress ipAddress;
		private File database;
		private DatabaseReader reader;
		private CountryResponse response;
		private Country country;
		private String country_name;
		private boolean exist;
		
		private int interval = 60;		
		private byte[] ip_ver = new byte[1];									
		private byte[] timestamp = new byte[4];			
		private byte[] caplen = new byte[4];
		private byte[] ipv4 = new byte[4];
		private byte[] ipv6 = new byte[16];
		private long Caplen;
		private long Timestamp;
		private byte[] value_bytes;
		
	    private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		
		public void configure(JobConf conf){
			interval = conf.getInt("pcap.record.rate.interval", 0);		
			dbDir = conf.getStrings("pcap.record.dbDir")[0];
			database = new File(dbDir+"GeoLite2-Country.mmdb");
			 try {
				reader = new DatabaseReader.Builder(database).build();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
				//获取IPV4地址
				System.arraycopy(value_bytes, 42, ipv4, 0, ipv4.length);
				src_ip = CommonData.longTostrIp(Bytes.toLong(ipv4));
				System.arraycopy(value_bytes, 46, ipv4, 0, ipv4.length);
				dst_ip = CommonData.longTostrIp(Bytes.toLong(ipv4));

				//获取所在国家
				ipAddress = InetAddress.getByName(src_ip);
				try {
					response = reader.country(ipAddress);
					exist = true;
				} catch (GeoIp2Exception e) {
					exist = false;
				}
				if(exist == true){
        			country = response.getCountry();
            		country_name = get_country(country.getName());
            		text.clear();
            		text.set("src\t" + Long.toString(Timestamp) + "\t" + country_name);
					longwrite.set(Caplen);
					output.collect(text, longwrite);	
				}else{
            		text.clear();
            		text.set("src\t" + Long.toString(Timestamp) + "\tOTHERS");
					longwrite.set(Caplen);
					output.collect(text, longwrite);
				}
				
				ipAddress = InetAddress.getByName(dst_ip);
				try {
					response = reader.country(ipAddress);
					exist = true;
				} catch (GeoIp2Exception e) {
					exist = false;
				}
				if(exist == true){
        			country = response.getCountry();
            		country_name = get_country(country.getName());
            		text.clear();
            		text.set("dst\t" + Long.toString(Timestamp) + "\t" + country_name);
					longwrite.set(Caplen);
					output.collect(text, longwrite);	
				}else{
            		text.clear();
            		text.set("dst\t" + Long.toString(Timestamp) + "\tOTHERS");
					longwrite.set(Caplen);
					output.collect(text, longwrite);
				}
				
			}else if(ip_ver[0]== PcapRec.IPV6){
				//获取IPV6地址
				System.arraycopy(value_bytes, 38, ipv6, 0, ipv6.length);
				src_ip = CommonData.BytesToStrIPv6(ipv6);
				System.arraycopy(value_bytes, 54, ipv6, 0, ipv6.length);
				dst_ip = CommonData.BytesToStrIPv6(ipv6);
	
				//获取所在国家
				ipAddress = InetAddress.getByName(src_ip);
				try {
					response = reader.country(ipAddress);
					exist = true;
				} catch (GeoIp2Exception e) {
					exist = false;
				}
				if(exist == true){
        			country = response.getCountry();
            		country_name = get_country(country.getName());
            		text.clear();
            		text.set("src\t" + Long.toString(Timestamp) + "\t" + country_name);
					longwrite.set(Caplen);
					output.collect(text, longwrite);	
				}else{
            		text.clear();
            		text.set("src\t" + Long.toString(Timestamp) + "\tOTHERS");
					longwrite.set(Caplen);
					output.collect(text, longwrite);
				}
				
				ipAddress = InetAddress.getByName(dst_ip);
				try {
					response = reader.country(ipAddress);
					exist = true;
				} catch (GeoIp2Exception e) {
					exist = false;
				}
				if(exist == true){
        			country = response.getCountry();
            		country_name = get_country(country.getName());
            		text.clear();
            		text.set("dst\t" + Long.toString(Timestamp) + "\t" + country_name);
					longwrite.set(Caplen);
					output.collect(text, longwrite);	
				}else{
            		text.clear();
            		text.set("dst\t" + Long.toString(Timestamp) + "\tOTHERS");
					longwrite.set(Caplen);
					output.collect(text, longwrite);
				}
				
			}else{//非ipv6或ipv4

			}
		}
	}
	
	public static class Reduce_Stats1 extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable> {	
		private long sum;
		private long count;
		private String temp;
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
		private String[] substring;
		private Text text = new Text();
		private LongWritable longwrite = new LongWritable();
		public void map
				(LongWritable key, Text value, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			
			substring = value.toString().split("\t");
			text.clear();
			text.set(substring[0]+"\t"+substring[1]+"\t"+substring[3]);
			longwrite.set(Long.parseLong(substring[4].trim()));
			output.collect(text, longwrite);
			
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
