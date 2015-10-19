/*
 * Decompiled with CFR 0_102.
 */
package com.offline.Package_IO;

import java.util.StringTokenizer;

import com.offline.Package_IO.Bytes;

public class CommonData {
    public static int strToHex(String str) {
        char[] arr = str.toCharArray();
        int i = 0;
        int val = 0;
        int tot = 0;
        if (str.startsWith("0x")) {
            i = 2;
        }
        while (i < arr.length) {
            tot<<=4;
            switch (arr[i]) {
                case 'A': 
                case 'a': {
                    val = 10;
                    break;
                }
                case 'B': 
                case 'b': {
                    val = 11;
                    break;
                }
                case 'C': 
                case 'c': {
                    val = 12;
                    break;
                }
                case 'D': 
                case 'd': {
                    val = 13;
                    break;
                }
                case 'E': 
                case 'e': {
                    val = 14;
                    break;
                }
                case 'F': 
                case 'f': {
                    val = 15;
                    break;
                }
                default: {
                    val = Integer.parseInt(String.valueOf(arr[i]));
                }
            }
            tot+=val;
            ++i;
        }
        return tot;
    }

    public static int strIpToInt(String str) {
        int val = 0;
        int i = 0;
        if (str == null) {
            return val;
        }
        StringTokenizer tok = new StringTokenizer(str, ".");
        int cnt = tok.countTokens();
        while (tok.hasMoreTokens()) {
            val+=Integer.parseInt(tok.nextToken()) << (cnt - i - 1) * 8;
            ++i;
        }
        return val;
    }

    public static String intTostrIp(int val) {
        int i = 3;
        int curval = 255;
        String ip = null;
        ip = String.valueOf(((long)val & 0xFFFFFFFFL) >> i * 8);
        while (i > 0) {
            ip = String.valueOf(ip) + "." + String.valueOf(((long)val & 0xFFFFFFFFL) >> 8 * --i & (long)curval);
        }
        return ip;
    }

    public static String longTostrIp(long lval) {
        int val = CommonData.LongToInt(lval);
        int i = 3;
        int curval = 255;
        String ip = null;
        ip = String.valueOf(((long)val & 0xFFFFFFFFL) >> i * 8);
        while (i > 0) {
            ip = String.valueOf(ip) + "." + String.valueOf(((long)val & 0xFFFFFFFFL) >> 8 * --i & (long)curval);
        }
        return ip;
    }

    public static int intTostrSubnet(int val) {
        return 32 - Integer.bitCount(val);
    }

    public static int longTostrSubnet(long lval) {
        int val = CommonData.LongToInt(lval);
        return 32 - Integer.bitCount(val);
    }

    public static int intTointSubnet(int val) {
        int retval = 0;
        for (int i = 0; i < 32 - val; ++i) {
            retval = retval << 1 | 1;
        }
        return retval;
    }

    public static int longTointSubnet(long lval) {
        int retval = 0;
        int val = CommonData.LongToInt(lval);
        for (int i = 0; i < 32 - val; ++i) {
            retval = retval << 1 | 1;
        }
        return retval;
    }

    public static long intToLong(int val) {
        return (long)val & 0xFFFFFFFFL;
    }

    public static int LongToInt(long val) {
        Long lval = new Long(val);
        return lval.intValue();
    }
    /////////////////////////////////
    public static String BytesToStrIPv6(byte[] value){
    	String ip = null;
    	ip = BytesToCell(value, 0);
    	for(int i = 1; i < 8; i++){
    		ip = ip + ":" + BytesToCell(value, i);
    	}
    	return ip;
    }
    
    public static String BytesToCell(byte[] value, int pos){
    	String cell = null;
    	byte temp;
    	int num;
    	temp = value[pos*2];
    	temp =(byte) ((temp>>4)&0x0f);
    	num = Bytes.toInt(temp);
    	cell = IntToWord(num);
    	temp = value[pos*2];
    	temp =(byte) (temp&0x0f);
    	num = Bytes.toInt(temp);
    	cell = cell + IntToWord(num);
    	
    	temp = value[pos*2+1];
    	temp =(byte) ((temp>>4)&0x0f);
    	num = Bytes.toInt(temp);
    	cell = cell + IntToWord(num);
    	temp = value[pos*2+1];
    	temp =(byte) (temp&0x0f);
    	num = Bytes.toInt(temp);
    	cell = cell + IntToWord(num);
    	
    	if(cell.substring(0, 0).equals("0")){
    		if(cell.substring(1, 1).equals("0")){
    			if(cell.substring(2, 2).equals("0")){
    				return cell.substring(3,3);
    			}
    			return cell.substring(2, 3);
    		}
    		return cell.substring(1,3);
    	}
    	return cell;
    }
    
    public static String IntToWord(int num){
    	if(0 <= num && num <= 9){
    		return Integer.toString(num);
    	}else if(num == 10)
    		return "a";
    	else if(num == 11)
    		return "b";
    	else if(num == 12)
    		return "c";
    	else if(num == 13)
    		return "d";
    	else if(num == 14)
    		return "e";
    	else 
    		return "f";
    	}
}

