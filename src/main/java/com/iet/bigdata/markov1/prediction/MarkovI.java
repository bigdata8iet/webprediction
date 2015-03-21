package com.iet.bigdata.markov1.prediction;

import org.apache.hadoop.hbase.util.Bytes;

public class MarkovI {
	private String userid = null;
	private String currentUrl = null;
	private String nextUrl = null;
	private static byte[] res1 = new byte[20];
	private static byte[] res2 = new byte[128];
	private static byte count;
	static int i = 0;
	private static byte[] res3 = new byte[128];

	public static byte getCount() {
		return count;
	}

	public static void setCount(byte count) {
		MarkovI.count = count;
	}

	public MarkovI() {
		userid = null;
		currentUrl = null;
		nextUrl = null;
	}

	public String getNextUrl() {
		return nextUrl;
	}

	public void setNextUrl(String nextUrl) {
		this.nextUrl = nextUrl;
	}

	public MarkovI(String userid, String currentUrl, String nextUrl) {
		this.userid = userid;
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

	@Override
	public String toString() {
		return "MarkovI [userid=" + userid + ", currentUrl=" + currentUrl
				+ ", nextUrl=" + nextUrl + ",counter=" + count + "]";
	}

	public static byte[] pack(byte[] result, String userid, String currentUrl,
			String nextUrl) {
		res1 = Bytes.toBytes(userid);
		res2 = Bytes.toBytes(currentUrl);
		res3 = Bytes.toBytes(nextUrl);
		for (i = 0; i < 20; i++) {
			result[i] = res1[i];
		}

		for (i = 0; i < 128; i++) {
			result[20 + i] = res2[i];
		}

		for (i = 0; i < 128; i++) {
			result[148 + i] = res3[i];
		}

		return result;
	}

	public static MarkovI unpack(byte[] result, byte[] result2) {

		for (i = 0; i < 20; i++) {
			res1[i] = result[i];
		}

		for (i = 0; i < 128; i++) {
			res2[i] = result[20 + i];
		}

		for (i = 0; i < 128; i++) {
			res3[i] = result2[i];
		}

		return new MarkovI(Bytes.toString(res1), Bytes.toString(res2),
				Bytes.toString(res3));
	}
}
