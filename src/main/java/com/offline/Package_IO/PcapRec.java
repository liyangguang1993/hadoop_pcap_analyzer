package com.offline.Package_IO;

public class PcapRec{
	public static final int IP_PROTO = 0x0800;		
	public static final int IPV4 = 0x40;		
	public static final int IPV6 = 0x60;		
	public static final int UDP = 17;	
	public static final int TCP = 6;	
	public static final int ICMP = 1;		
	public static final int ICMPV6 = 58;		
	
	public static final int POS_TSTAMP = 0;
	public static final int LEN_TSTAMP = 4;
	
	public static final int POS_CAPLEN = 8;
	public static final int LEN_CAPLEN = 4;
	
	public static final int POS_ETH_TYPE = 28;		//协议类型的位置
	public static final int LEN_ETH_TYPE = 2;			//协议的长度
	
	public static final int POS_IP_VER = 30;				//IP协议类型
	public static final int LEN_IP_VER = 1;				//长度
	
	public static final int IPV4_IP_LEN = 4;
	public static final int IPV4_POS_SIP = 42;
	public static final int IPV4_POS_DIP = 42;
	
	public static final int IPV6_IP_LEN = 16;
	public static final int IPV6_POS_SIP = 38;
	public static final int IPV6_POS_DIP = 54;
	
	public static final int POS_IPV4_BYTES = 32;		//IP包的长度的位置
	public static final int POS_IPV6_BYTES = 34;       //IPV6数据包的长度的位置
	public static final int LEN_IPV4_BYTES = 2;			
	public static final int LEN_IPV6_BYTES = 4;
	
	public static final int POS_OPT = 30;			//IPV4可选项
	public static final int LEN_OPT = 1;		
	
	public static final int LEN_PORT = 4;		
	public static final int POS_IPV4_SP = 50;		
	public static final int POS_IPV4_DP = 52;		
	public static final int POS_IPV6_SP = 70;		
	public static final int POS_IPV6_DP = 72;		
	
	public static final int POS_IPV4_PROTO = 39;
	public static final int LEN_IPV4_PROTO = 1;					//protocol length 
	public static final int POS_IPV6_PROTO = 36;
	public static final int LEN_IPV6_PROTO = 1;					//protocol length 
	
	public static final int POS_SIP = 42;					//source IP
	public static final int POS_HL_IPV4 = 30;			
	public static final int POS_HL_IPV6 = 70;			
	public static final int POS_DIP = 46;					//destination IP
	public static final int POS_PT = 39;						//protocol type
	public static final int POS_SP = 50;						//source port
	public static final int POS_DP = 52;						//destination port
	public static final int ICMP_TC = 50;					//
	public static final int POS_HTTP = 70;				//
	
	public static final int POS_TSTMP = 0;				//time stamp
	
	public static final int LEN_IPADDR=4;				//IP address length
	
	//-----------------------------------//
	public static final int LEN_VAL1 = 4;	
	public static final int POS_VAL = 2;
	
	public static final int LEN_VAL2 = LEN_VAL1*2;
	public static final int LEN_VAL3 = LEN_VAL1*3;	
	public static final int POS_V_BC = POS_VAL;		
	public static final int POS_V_PC = LEN_VAL1+POS_VAL;
//	public static final int POS_V_FC = LEN_VAL2+POS_VAL;
}
