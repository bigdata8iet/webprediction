package com.iet.bigdata.markov2.prediction;

import java.util.Iterator;
import java.util.Set;

public class PredictionResultMarkovII {

	public static void main(String[] args) {
		
		byte[] result1=new byte[404];
		byte[] result2=new byte[404];
		Set <MarkovII> mk=null;
		
		MarkovII mk1=new MarkovII();
		mk1.setUserid("jraikwal");
		mk1.setcurrentUrl("radarfeed.moneycontrol.com/mcradar/processing.php?");
		mk1.setprevUrl("bcp.crwdcntrl.net/px?");
		mk1.setNextUrl("");
		result1=mk1.pack(result1);
		
		MarkovII mk2=new MarkovII();
		mk2.setUserid("jraikwal");
		mk2.setprevUrl("bcp.crwdcntrl.net/px?"+1);
		mk2.setcurrentUrl(mk1.getcurrentUrl());
		mk2.setNextUrl("");
		result2=mk2.pack(result2);
		
		try {
			
			mk=MarkovIIHBaseOperations.getPrediction(result1 , result2);
			Iterator<MarkovII> itr=mk.iterator();
			
			while(itr.hasNext()){
				mk1=itr.next();
				System.out.println("PredictedNextUrls: "+mk1.getNextUrl().trim()+"\tCount"+mk1.getCount());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
