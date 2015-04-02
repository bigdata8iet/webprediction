package com.iet.bigdata.loganalysis.DomainConsumption;

import org.apache.hadoop.hbase.util.Bytes;

public class DomainConsumption {

	private int date;
	private String domain;
	private long value;
	private byte[] key=new byte[24];
	private byte[] dom=new byte[20];
	private byte[] usage=new byte[8];
	static int i;
	
	public int getDate() {
		return date;
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
		return "DomainConsumption [Year=" + (date/100) +"\tMonth=" + (date%100) +  "\tdomain=" + domain
				+ "\tUsage=" + value + "]";
	}

	public byte[] packKey(){
		key[0] = (byte) (date >> 24);
		key[1] = (byte) (date >> 16);
		key[2] = (byte) (date >> 8);
		key[3] = (byte) (date);
		
		dom=Bytes.toBytes(domain);
		for(i=0;i<20;i++){
			key[4+i]=dom[i];
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
		date=((bytes[0] & 0xff) << 24) 
		| ((bytes[1] & 0xff) << 16)
		| ((bytes[2] & 0xff) << 8) 
		| (bytes[3] & 0xff);
		
		this.setDate(date);
		
		for(i=0;i<20;i++){
			dom[i]=bytes[4+i];
		}
		this.setDomain(Bytes.toString(dom));
	}

	public void unpackUsage(byte[] bytes){
		value = ((bytes[0] & 0xff) << 56) 
				| ((bytes[1] & 0xff) << 48)
				| ((bytes[2] & 0xff) << 40) 
				| (bytes[3] & 0xff) << 32
				|((bytes[4] & 0xff) << 24) 
				| ((bytes[5] & 0xff) << 16)
				| ((bytes[6] & 0xff) << 8) 
				| (bytes[7] & 0xff);
	}

}
