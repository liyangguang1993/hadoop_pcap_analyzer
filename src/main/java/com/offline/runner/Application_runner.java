package com.offline.runner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import com.offline.analyzer.Application_analyzer;

public class Application_runner {
	static final String INPATH = "/hduser/sample_input";
	static final String OUTPATH = "/hduser/application_result";
	
	static JobConf conf = new JobConf(Application_analyzer.class);

	static String getFilterFromFile(String filename){		
		return null;
	}
	
	public static void main(String[] args) throws Exception{
		String srcFilename = new String();		
		String dstFilename= OUTPATH+"/";
		int topN = 0;
		int period = 24;
		int interval = 60;
		boolean rtag = false;
		char argtype = 0;
		
		conf.setInt("pcap.record.rate.interval", interval);//默认时间单位：60秒
		conf.setInt("pcap.record.rate.period", period);//默认时间长度：24小时
		/* Argument Parsing */
		int i = 0;
		while(i<args.length){
			if(args[i].startsWith("-")){
				
				argtype = args[i].charAt(1);
				switch (argtype){
				
				case 'R': case 'r':
					srcFilename += args[i].substring(2);
					rtag = true;
					break;		
					
				case 'D': case 'd':
					dstFilename += args[i].substring(2);
					break;			
					
				case 'I': case 'i':
					interval = Integer.parseInt(args[i].substring(2).trim());
					conf.setInt("pcap.record.rate.interval", interval);
					break;	
					
				case 'P': case 'p':
					period = Integer.parseInt(args[i].substring(2).trim());
					conf.setInt("pcap.record.rate.period", period);
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
				case 'R': case 'r':
					srcFilename += args[i];
					rtag = true;
					break;		
					
				case 'D': case 'd':
					dstFilename += args[i];
					break;			
					
				case 'I': case 'i':
					interval = Integer.parseInt(args[i].substring(1).trim());
					conf.setInt("pcap.record.rate.interval", interval);
					break;	
					
				case 'P': case 'p':
					period = Integer.parseInt(args[i].substring(1).trim());
					conf.setInt("pcap.record.rate.period", period);
					break;
					
				case 'N': case 'n': // topN
					topN = Integer.parseInt(args[i].trim());
					break;
					
				default:
					;
				break;
				}
			}
			i++;
		}
		
		conf.setInt("pcap.record.sort.topN", topN);
		
		if(rtag==false) 
			srcFilename = INPATH+"/";
		
		Path inputPath = new Path(srcFilename);
			
		System.out.println(" begin"+srcFilename);
		
		Application_analyzer analyzer = new Application_analyzer(conf);
		analyzer.start(inputPath, dstFilename);
			
		System.out.println("finished");
		
	}
}
