package com.offline.runner;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import com.offline.analyzer.Network_analyzer;

public class Network_runner {
	static final String INPATH = "/hduser/sample_input";
	static final String OUTPATH = "/hduser/network_result";
	static final String DBPATH = "/home/seasun/下载";
	
	static JobConf conf = new JobConf(Network_analyzer.class);

	static String getFilterFromFile(String filename){		
		return null;
	}

	public static void main(String[] args) throws Exception{
		String srcFilename=INPATH+"/";		
		String dstFilename=OUTPATH+"/";
		String dbFilename =DBPATH+"/";
		int topN = 0;
		int period = 24;
		int interval = 60;
		long cap_start = Long.MAX_VALUE;
		long cap_end = Long.MIN_VALUE;
		char argtype = 0;
		
		conf.setInt("pcap.record.rate.interval", interval);//默认时间单位：60秒
		conf.setInt("pcap.record.rate.period", period);//默认时间长度：24小时
		
		int i = 0;
		while(i<args.length){
			if(args[i].startsWith("-")){
				
				argtype = args[i].charAt(1);
				switch (argtype){
				case 'A': case 'a':
					dbFilename = args[i].substring(2);
					break;
					
				case 'R': case 'r':
					srcFilename = args[i].substring(2);
					break;		
					
				case 'D': case 'd':
					dstFilename = args[i].substring(2);
					break;			
					
				case 'I': case 'i':
					interval = Integer.parseInt(args[i].substring(2).trim());
					conf.setInt("pcap.record.rate.interval", interval);
					break;	
					
				case 'P': case 'p':
					period = Integer.parseInt(args[i].substring(2).trim());
					conf.setInt("pcap.record.rate.period", period);
					System.out.println(period);
					break;
					
				case 'N': case 'n': // topN
					topN = Integer.parseInt(args[i].substring(2).trim());
					break;
					
				default:
					;
				break;
				}					
			}
			else{
				argtype = args[i].charAt(0);
				switch (argtype){
				case 'A': case 'a':
					dbFilename = args[i].substring(1);
					break;
					
				case 'R': case 'r':
					srcFilename = args[i].substring(1);
					break;		
					
				case 'D': case 'd':
					dstFilename = args[i].substring(1);
					break;			
					
				case 'I': case 'i':
					interval = Integer.parseInt(args[i].substring(1).trim());
					conf.setInt("pcap.record.rate.interval", interval);
					break;	
					
				case 'P': case 'p':
					period = Integer.parseInt(args[i].substring(1).trim());
					conf.setInt("pcap.record.rate.period", period);
					System.out.println(period);
					break;
					
				case 'N': case 'n': // topN
					topN = Integer.parseInt(args[i].substring(1).trim());
					break;
					
				default:
					;
				break;
				}
			}
			i++;
		}
		
		conf.setInt("pcap.record.sort.topN", topN);
		conf.setStrings("pcap.record.dbDir", dbFilename);
		
		Path inputPath = new Path(srcFilename);
			
		System.out.println(" begin\nsource:"+srcFilename);
		
		Date date = new Date();
		long time = date.getTime();
		System.out.println(time);
		System.out.println(time);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH");
		String Date = sdf.format(new Date(time));
		System.out.println(Date);
		time =  time-(time+8*60*60*1000)%(period*60*60*1000);
		System.out.println(time);
		Date = sdf.format(new Date(time));
		System.out.println(Date);
		
		Network_analyzer analyzer = new Network_analyzer(conf);
		analyzer.start(inputPath, dstFilename, cap_start, cap_end);
		
		System.out.println("finished");
		
	}
}
