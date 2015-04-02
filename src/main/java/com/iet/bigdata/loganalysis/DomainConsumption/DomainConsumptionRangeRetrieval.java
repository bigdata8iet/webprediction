package com.iet.bigdata.loganalysis.DomainConsumption;

import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

import com.iet.bigdata.markov1.prediction.MarkovI;


public class DomainConsumptionRangeRetrieval {
public static void main(String args[]){
	
	byte[] result1=null;
	byte[] result2=null;
	Set <DomainConsumption> set=null;
	
	DomainConsumption uc=new DomainConsumption();
	DomainConsumption uc1=new DomainConsumption();
	Scanner sc=new Scanner(System.in);
		
	uc.setDomain(String.format("%-20s",""));
	uc1.setDomain(String.format("%-20s",""));
	
	System.out.println("Enter Start Year");
	uc.setDate(sc.nextInt());
	
	System.out.println("Enter Start Month");
	uc.setDate((uc.getDate()*100 + sc.nextInt()));

	System.out.println("Enter End Year");
	uc1.setDate(sc.nextInt());
	
	System.out.println("Enter End Month");
	uc1.setDate((uc1.getDate()*100 + sc.nextInt()));
	
	System.out.println(uc);
	System.out.println(uc1);
	
	result1=uc.packKey();
	result2=uc1.packKey();

	try {
		
		set=HBaseOperations.getSelectedInfo(result1 , result2);
		Iterator<DomainConsumption> itr=set.iterator();
		
		while(itr.hasNext()){
			uc1=itr.next();
			System.out.println(uc1);
		}
	} catch (Exception e) {
		e.printStackTrace();
	}

} 
}
