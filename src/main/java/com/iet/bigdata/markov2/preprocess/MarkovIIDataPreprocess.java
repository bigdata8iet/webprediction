package com.iet.bigdata.markov2.preprocess;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class MarkovIIDataPreprocess {

	public static void main(String[] args) {
		String line = null, arr[] = null;
		String new_line = null;
		String previousURL = null;
		String currentURL = null;

		try {

			FileInputStream fs = new FileInputStream(
					"/mnt/data/workspace/prediction/test/preprocess_markov1.txt");

			PrintWriter writer = new PrintWriter(
					"/mnt/data/workspace/prediction/test/preprocess_markov2.txt");

			BufferedReader br = new BufferedReader(new InputStreamReader(fs));
			Map<String, String> userPreviousMap = new HashMap<String, String>();
			Map<String, String> userCurrentUrlMap = new HashMap<String, String>();

			while ((line = br.readLine()) != null) {
				arr = line.split(",");

				previousURL = userPreviousMap.get(arr[0]);
				userPreviousMap.put(arr[0], arr[1]);
				currentURL = userCurrentUrlMap.get(arr[0]);
				userCurrentUrlMap.put(arr[0], arr[2]);

				if (previousURL == null || currentURL == null)
					continue;

				new_line = arr[0] + "," + previousURL + "," + currentURL + ","
						+ arr[1] + "\n";

				writer.write(new_line);
				System.out.print(new_line);

			}

			br.close();
			writer.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
