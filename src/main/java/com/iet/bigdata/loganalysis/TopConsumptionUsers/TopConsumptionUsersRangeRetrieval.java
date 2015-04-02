package com.iet.bigdata.loganalysis.TopConsumptionUsers;

import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

import com.iet.bigdata.markov1.prediction.MarkovI;


public class TopConsumptionUsersRangeRetrieval {
	public static void main(String args[]){
	
		byte[] result1=new byte[44];
		byte[] result2=new byte[44];
		Set <TopConsumptionUsers> set=null;
	
		TopConsumptionUsers uc=new TopConsumptionUsers();
		TopConsumptionUsers uc1=new TopConsumptionUsers();
	
		Scanner sc=new Scanner(System.in);
		System.out.println("Enter Start Year");
		uc.setDate(sc.nextInt());
	
		System.out.println("Enter Start Month");
		uc.setDate((uc.getDate()*100 + sc.nextInt()));

		System.out.println("Enter End Year");
		uc1.setDate(sc.nextInt());
	
		System.out.println("Enter End Month");
		uc1.setDate((uc1.getDate()*100 + sc.nextInt()));
	
		uc.setUser(String.format("%-20s", ""));
		uc1.setUser(uc.getUser());
		
		result1=uc.packKey();
		result2=uc1.packKey();
	
		try {
		
			set=HBaseOperations.getTopConsumers(result1 , result2);
			Iterator<TopConsumptionUsers> itr=set.iterator();
			
			while(itr.hasNext()){
				uc1=itr.next();
				System.out.println(uc1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}
