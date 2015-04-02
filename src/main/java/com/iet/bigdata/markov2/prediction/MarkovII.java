package com.iet.bigdata.markov2.prediction;

import org.apache.hadoop.hbase.util.Bytes;

import com.iet.bigdata.markov1.prediction.MarkovI;

public class MarkovII {

	private String userid = null;
	private String prevUrl = null;
	private String currentUrl = null;
	private String nextUrl = null;
	
	private static byte[] res1 = new byte[20];
	private static byte[] res2 = new byte[128];
	private static byte[] res3 = new byte[128];
	private static byte[] res4 = new byte[128];
	
	private  byte count;
	static int i = 0;

	public byte getCount() {
		return count;
	}

	public void setCount(byte count) {
		this.count = count;
	}

	public MarkovII() {
		userid = null;
		currentUrl = null;
		nextUrl = null;
		prevUrl=null;
	}

	public String getNextUrl() {
		return nextUrl;
	}

	public void setNextUrl(String nextUrl) {
		this.nextUrl = nextUrl;
	}

	public MarkovII(String userid, String prevUrl, String currentUrl, String nextUrl) {
		this.userid = userid;
		this.prevUrl=prevUrl;
		this.currentUrl = currentUrl;
		this.nextUrl = nextUrl;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getcurrentUrl() {
		return currentUrl;
	}

	public void setcurrentUrl(String currentUrl) {
		this.currentUrl = currentUrl;
	}

	public String getprevUrl() {
		return prevUrl;
	}

	public void setprevUrl(String prevUrl) {
		this.prevUrl = prevUrl;
	}
	
	@Override
	public String toString() {
		return "MarkovII [userid=" + userid + ", Previous Url=" + prevUrl + ", Current Url=" + currentUrl 
				+ ", Next Url=" + nextUrl + ",counter=" + count + "]";
	}

	public static byte[] pack(byte[] result, String userid,String prevUrl, String currentUrl,
			String nextUrl) {
		res1 = Bytes.toBytes(userid);
		res3 = Bytes.toBytes(prevUrl);
		res2 = Bytes.toBytes(currentUrl);
		res4 = Bytes.toBytes(nextUrl);
		for (i = 0; i < 20; i++) {
			result[i] = res1[i];
		}

		for (i = 0; i < 128; i++) {
			result[20 + i] = res2[i];
			result[148 + i] = res3[i];
			result[276+i]=res4[i];
		}

		return result;
	}

	public byte[] pack(byte[] result) {
		
		if (userid.length() < 20 )
			userid = String.format("%-20s", userid);

		// Converting url to fixed length
		if (prevUrl.length() < 128)
			prevUrl = String.format("%-128s", prevUrl);
		else if (prevUrl.length() > 128)
			prevUrl = prevUrl.substring(0, 127);

		
		if (currentUrl.length() < 128)
			currentUrl = String.format("%-128s", currentUrl);
		else if (currentUrl.length() > 128)
			currentUrl = currentUrl.substring(0, 127);

		if (nextUrl.length() < 128)
			nextUrl = String.format("%-128s", nextUrl);
		else if (nextUrl.length() > 128)
			nextUrl = nextUrl.substring(0, 127);

		res1 = Bytes.toBytes(userid);
		res2 = Bytes.toBytes(prevUrl);
		res3 = Bytes.toBytes(currentUrl);
		res4 = Bytes.toBytes(nextUrl);
		
		for (i = 0; i < 20; i++) {
			result[i] = res1[i];
		}

		for (i = 0; i < 128; i++) {
			result[20 + i] = res3[i];
			result[148 + i] = res2[i];
			result[276+i]=res4[i];
		}

		return result;
	}
	
	public static MarkovII unpack(byte[] result) {

		for (i = 0; i < 20; i++) {
			res1[i] = result[i];
		}

		for (i = 0; i < 128; i++) {
			res2[i] = result[20 + i];	//currentURL
			res3[i] = result[148+i];	//prevURL
			res4[i] = result[276+i];	//nextURL
		}

		return new MarkovII(Bytes.toString(res1), Bytes.toString(res3),
				Bytes.toString(res2),Bytes.toString(res4));
	}
}