package com.offline.Hive.SerDe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import com.offline.Package_IO.*;

public class PcapSerDe implements Deserializer {
	ObjectInspector inspector;
	ArrayList<Object> row;
	int numColumns;
	List<String> columnNames;
	
	private int interval = 60;
	private byte[] ip_ver = new byte[1];											//ip协议版本
	private byte[] proto = new byte[1];								//协议类型
	private byte[] timestamp = new byte[4];					//开始时间
	private byte[] caplen = new byte[4];
	private byte[] port = new byte[2];
	private byte[] hlen = new byte[1];
	private byte[] ipv4 = new byte[4];
	private byte[] ipv6 = new byte[16];
	private String Ip_ver;
	private String Proto;
	private long Caplen;
	private long Timestamp;
	private String SIP;
	private String DIP;
	private int SP;
	private int DP;
	private int Optlen;
	
	
	public SerDeStats getSerDeStats() {
		//We collect no statistics.
		return new SerDeStats();
	}

	public void initialize(Configuration cfg, Properties props) throws SerDeException {		
		String columnNameProperty = props.getProperty(Constants.LIST_COLUMNS);
		columnNames = Arrays.asList(columnNameProperty.split(","));
		numColumns = columnNames.size();

		String columnTypeProperty = props.getProperty(Constants.LIST_COLUMN_TYPES);
		List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

		// Ensure we have the same number of column names and types
		assert numColumns == columnTypes.size();

        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(numColumns);
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++) {
        	ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            inspectors.add(oi);
            row.add(null);
        }
        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
	}

	public Object deserialize(Writable w) throws SerDeException {
		/*ObjectWritable obj = (ObjectWritable)w;
		Packet packet = (Packet)obj.get();

        for (int i = 0; i < numColumns; i++) {
            String columName = columnNames.get(i);
            Object value = packet.get(columName);
           	row.set(i, value);
        }*/
        
        BytesWritable values = (BytesWritable)w;
        byte[] value_bytes = values.getBytes();
        
        System.arraycopy(value_bytes, PcapRec.POS_TSTAMP, timestamp, 0 , PcapRec.LEN_TSTAMP);	
		Timestamp = Bytes.toLong(BinaryUtils.flipBO(timestamp,4));
		Timestamp = Timestamp - (Timestamp%interval);
		System.arraycopy(value_bytes, PcapRec.POS_CAPLEN, caplen, 0 , PcapRec.LEN_CAPLEN);	
		Caplen = Bytes.toInt(BinaryUtils.flipBO(caplen, 4));
		System.arraycopy(value_bytes, PcapRec.POS_IP_VER, ip_ver, 0, PcapRec.LEN_IP_VER);
		 ip_ver[0] = (byte) (ip_ver[0]&0xf0);	
		 
		 if(ip_ver[0]== PcapRec.IPV4){
			 	Ip_ver = "IPV4";
			 	
				System.arraycopy(value_bytes, 42, ipv4, 0, ipv4.length);
				SIP = CommonData.longTostrIp(Bytes.toLong(ipv4));
				System.arraycopy(value_bytes, 46, ipv4, 0, ipv4.length);
				DIP = CommonData.longTostrIp(Bytes.toLong(ipv4));
				
				System.arraycopy(value_bytes, PcapRec.POS_IPV4_PROTO, proto, 0 , PcapRec.LEN_IPV4_PROTO);	
				if(Bytes.toInt(proto) == PcapRec.ICMP){
					Proto = "ICMP";
				}else if(Bytes.toInt(proto) == PcapRec.TCP){
					Proto = "TCP";
				}else if(Bytes.toInt(proto) == PcapRec.UDP){
					Proto = "UDP";
				}else{
					Proto = "UNKNOW";
				}
				if(Proto.equals("ICMP")||Proto.equals("UNKNOW")){
					SP = 0;
					DP = 0;
				}else{
					System.arraycopy(value_bytes, PcapRec.POS_HL_IPV4, hlen, 0, hlen.length);					
					Optlen = (hlen[0] & 0x0f)*4 - 20;
					System.arraycopy(value_bytes, PcapRec.POS_IPV4_SP+Optlen, port, 0, port.length);
					SP = Bytes.toInt(port);
					System.arraycopy(value_bytes, PcapRec.POS_IPV4_DP+Optlen, port, 0, port.length);
					DP = Bytes.toInt(port);
				}
			}else if(ip_ver[0] == PcapRec.IPV6){
				Ip_ver = "IPV6";

				System.arraycopy(value_bytes, 38, ipv6, 0, ipv6.length);
				SIP = CommonData.BytesToStrIPv6(ipv6);
				System.arraycopy(value_bytes, 54, ipv6, 0, ipv6.length);
				DIP = CommonData.BytesToStrIPv6(ipv6);
				
				System.arraycopy(value_bytes, PcapRec.POS_IPV6_PROTO, proto, 0 , PcapRec.LEN_IPV6_PROTO);	
				if(Bytes.toInt(proto) == PcapRec.ICMPV6){
					Proto = "ICMP";
				}else if(Bytes.toInt(proto) == PcapRec.TCP){
					Proto = "TCP";
				}else if(Bytes.toInt(proto) == PcapRec.UDP){
					Proto = "UDP";
				}else{
					Proto = "UNKNOW";
				}
				if(Proto.equals("ICMP")||Proto.equals("UNKNOW")){
					SP = 0;
					DP = 0;
				}else{
					System.arraycopy(value_bytes, PcapRec.POS_IPV6_SP, port, 0, port.length);
					SP = Bytes.toInt(port);
					System.arraycopy(value_bytes, PcapRec.POS_IPV6_DP, port, 0, port.length);
					DP = Bytes.toInt(port);
				}
     		
			}else{
				Ip_ver = "OTHER";
				Proto = "UNKNOW";
				SIP = null;
				DIP = null;
				SP = 0;
				DP = 0;
			}
		 
		 String columName ;
		 Object value;
		 
        for(int i = 0; i < numColumns; i++){
        	columName = columnNames.get(i);
        	
        	if(columName.equals("pro")){
        		value = Proto;
        	}else if(columName.equals("sip")){
        		value = SIP;
        	}else if(columName.equals("dip")){
        		value = DIP;
        	}else if(columName.equals("sp")){
        		value = SP;
        	}else if(columName.equals("dp")){
        		value = DP;
        	}else if(columName.equals("len")){
        		value = Caplen;
        	}else if(columName.equals("tstamp")){
        		value = Timestamp;
        	}else{
        		value = columName;
        	}
        	row.set(i, value);
        }
        
        return row;
	}

	public ObjectInspector getObjectInspector() throws SerDeException {
		return inspector;
	}
}