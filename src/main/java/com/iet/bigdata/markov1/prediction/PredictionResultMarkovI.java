package com.iet.bigdata.markov1.prediction;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.iet.bigdata.markov1.prediction.MarkovI;
import com.iet.bigdata.markov1.prediction.MarkovIHBaseOperations;

public class PredictionResultMarkovI {

	public static void main(String[] args) {
		byte[] result1=new byte[276];
		byte[] result2=new byte[276];
		Set <MarkovI> mk=null;
		
		MarkovI mk1=new MarkovI();
		mk1.setUserid("jraikwal");
		mk1.setcurrentUrl("radarfeed.moneycontrol.com/mcradar/processing.php?");
		mk1.setNextUrl("");
		result1=mk1.pack(result1);
		
		MarkovI mk2=new MarkovI();
		mk2.setUserid("jraikwal");
		mk2.setcurrentUrl("radarfeed.moneycontrol.com/mcradar/processing.php?");
		mk2.setcurrentUrl(mk2.getcurrentUrl() + 1);
		mk2.setNextUrl("");
		result2=mk2.pack(result2);
		
		try {
			
			mk=MarkovIHBaseOperations.getPrediction(result1 , result2);
			Iterator<MarkovI> itr=mk.iterator();
			
			while(itr.hasNext()){
				mk1=itr.next();
				System.out.println("PredictedNextUrls: "+mk1.getNextUrl().trim()+"\tCount"+mk1.getCount());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
