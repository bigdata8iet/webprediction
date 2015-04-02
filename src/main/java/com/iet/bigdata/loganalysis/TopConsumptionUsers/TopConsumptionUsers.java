package com.iet.bigdata.loganalysis.TopConsumptionUsers;

import org.apache.hadoop.hbase.util.Bytes;

public class TopConsumptionUsers {

	private int date;
	private String user;
	private long value;
	
	private byte[] key=new byte[32];
	private byte[] temp=new byte[20];
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

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	
	@Override
	public String toString() {
		return "UserConsumption [Year=" + (date/100) +"\tMonth=" + (date%100) + 
				"\tUser=" + user + "\tUsage=" + value + "]";
	}

	public byte[] packKey(){
		
		key[0] = (byte) (date >> 24);
		key[1] = (byte) (date >> 16);
		key[2] = (byte) (date >> 8);
		key[3] = (byte) (date);

		key[4] = (byte) (value >> 56);
		key[5] = (byte) (value >> 48);
		key[6] = (byte) (value >> 40);
		key[7] = (byte) (value >> 32);
		key[8] = (byte) (value >> 24);
		key[9] = (byte) (value >> 16);
		key[10] = (byte) (value >> 8);
		key[11] = (byte) (value);
		
		temp=Bytes.toBytes(user);
		for(i=0;i<20;i++){
			key[12+i]=temp[i];
		}
				
		return key;	
	}
		
	public void unpackKey(byte[] bytes){
		
		date=((bytes[0] & 0xff) << 24) 
		| ((bytes[1] & 0xff) << 16)
		| ((bytes[2] & 0xff) << 8) 
		| (bytes[3] & 0xff);
		
		this.setDate(date);
		
		value = ((bytes[4] & 0xff) << 56) 
				| ((bytes[5] & 0xff) << 48)
				| ((bytes[6] & 0xff) << 40) 
				| ((bytes[7] & 0xff) << 32)
				| ((bytes[8] & 0xff) << 24) 
				| ((bytes[9] & 0xff) << 16)
				| ((bytes[10] & 0xff) << 8) 
				| (bytes[11] & 0xff);

		this.setValue(value);
		
		for(i=0;i<20;i++){
			temp[i]=bytes[12+i];
		}
		this.setUser(Bytes.toString(temp));
	}

}