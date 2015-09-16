package com.offline.Package_IO;

public class PcapRec{
	public static final int IP_PROTO = 0x0800;		
	public static final int IPV4 = 0x40;		
	public static final int UDP = 17;	
	public static final int TCP = 6;	
	public static final int ICMP = 1;		
	
	public static final int POS_ETH_TYPE = 28;		//协议类型的位置
	public static final int LEN_ETH_TYPE = 2;			//协议的长度
	public static final int POS_IP_VER = 30;				//IP协议类型
	public static final int LEN_IP_VER = 1;				//长度
	
	public static final int POS_IP_BYTES = 32;		//IP包的长度的位置？？？
	public static final int POS_IPV6_BYTES = 34;  //IPV6数据包的长度的位置？？？
	public static final int LEN_IP_BYTES = 2;			//
	
	public static final int POS_SIP = 42;					//source IP
	public static final int POS_HL = 30;						//包头长度？？？？？
	public static final int POS_DIP = 46;					//destination IP
	public static final int POS_PT = 39;						//protocol type
	public static final int POS_SP = 50;						//source port
	public static final int POS_DP = 52;						//destination port
	public static final int ICMP_TC = 50;					//
	public static final int POS_HTTP = 70;				//
	
	public static final int POS_TSTMP = 0;				//time stamp
	
	public static final int LEN_IPADDR=4;				//IP address length
	public static final int LEN_PORT=2;						//port length
	public static final int LEN_PROTO=1;					//protocol length 
	
	//-----------------------------------//
	public static final int LEN_VAL1 = 4;	
	public static final int POS_VAL = 2;
	
	public static final int LEN_VAL2 = LEN_VAL1*2;
	public static final int LEN_VAL3 = LEN_VAL1*3;	
	public static final int POS_V_BC = POS_VAL;		
	public static final int POS_V_PC = LEN_VAL1+POS_VAL;
//	public static final int POS_V_FC = LEN_VAL2+POS_VAL;
}
