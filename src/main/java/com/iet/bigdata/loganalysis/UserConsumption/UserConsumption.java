package com.iet.bigdata.loganalysis.UserConsumption;

import org.apache.hadoop.hbase.util.Bytes;

public class UserConsumption {

	private int date;
	private String domain;
	private String user;
	private long value;
	
	private byte[] key=new byte[44];
	private byte[] temp=new byte[20];
	private byte[] usage=new byte[8];
	static int i;
	
	public int getDate() {
		return date;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setDate(int date) {
		this.date = date;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	
	@Override
	public String toString() {
		return "UserConsumption [Year=" + (date/100) +"\tMonth=" + (date%100) + 
				"\tUser=" + user+"\tDomain=" + domain + "\tUsage=" + value + "]";
	}

	public byte[] packKey(){
		temp=Bytes.toBytes(user);
		for(i=0;i<20;i++){
			key[i]=temp[i];
		}

		
		key[20] = (byte) (date >> 24);
		key[21] = (byte) (date >> 16);
		key[22] = (byte) (date >> 8);
		key[23] = (byte) (date);
		
		temp=Bytes.toBytes(domain);
		for(i=0;i<20;i++){
			key[24+i]=temp[i];
		}

		return key;	
	}
	
	public byte[] packUsage(){
		
		usage[0] = (byte) (value >> 56);
		usage[1] = (byte) (value >> 48);
		usage[2] = (byte) (value >> 40);
		usage[3] = (byte) (value >> 32);
		usage[4] = (byte) (value >> 24);
		usage[5] = (byte) (value >> 16);
		usage[6] = (byte) (value >> 8);
		usage[7] = (byte) (value);
		return usage;
	}
	
	public void unpackKey(byte[] bytes){
		
		for(i=0;i<20;i++){
			temp[i]=bytes[i];
		}
		this.setUser(Bytes.toString(temp));

		date=((bytes[20] & 0xff) << 24) 
		| ((bytes[21] & 0xff) << 16)
		| ((bytes[22] & 0xff) << 8) 
		| (bytes[23] & 0xff);
		
		this.setDate(date);
		
		for(i=0;i<20;i++){
			temp[i]=bytes[24+i];
		}
		this.setDomain(Bytes.toString(temp));
	}

	public void unpackUsage(byte[] bytes){
		value = ((bytes[0] & 0xff) << 56) 
				| ((bytes[1] & 0xff) << 48)
				| ((bytes[2] & 0xff) << 40) 
				| ((bytes[3] & 0xff) << 32)
				| ((bytes[4] & 0xff) << 24) 
				| ((bytes[5] & 0xff) << 16)
				| ((bytes[6] & 0xff) << 8) 
				| (bytes[7] & 0xff);
	}

}
