package com.iet.bigdata.markov1.preprocess;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class MarkovIDataPreprocess {

	public static void main(String[] args) {
		String line = null, arr[] = null;
		String new_line = null;
		try {

			FileInputStream fs = new FileInputStream(
					"/mnt/data/workspace/prediction/test/proxy_out");

			PrintWriter writer = new PrintWriter(
					"/mnt/data/workspace/prediction/test/preprocess_markov1.txt");

			BufferedReader br = new BufferedReader(new InputStreamReader(fs));
			Map<String, String> userCurrentUrlMap = new HashMap<String, String>();

			while ((line = br.readLine()) != null) {
				arr = line.split(" ");
				arr[6] = arr[6].replaceFirst("(?:https://)", "")
						.replaceFirst("(?:http://)", "")
						.replaceFirst("(?:www.)", "")
						.replaceFirst("(?:ww.)", "");

				if (arr[5].equalsIgnoreCase("get")
						&& (!arr[7].equalsIgnoreCase("-"))
						&& arr[9].equalsIgnoreCase("text/html")) {

					// Padding spaces in user id
					if (arr[7].length() < 20 && (!arr[7].equalsIgnoreCase("-")))
						arr[7] = String.format("%-20s", arr[7]);

					// Converting url to fixed length
					if (arr[6].length() < 128)
						arr[6] = String.format("%-128s", arr[6]);
					else if (arr[6].length() > 128)
						arr[6] = arr[6].substring(0, 127);

					String previousURL = userCurrentUrlMap.get(arr[7]);
					userCurrentUrlMap.put(arr[7], arr[6]);

					if (previousURL == null)
						continue;

					new_line = arr[7] + "," + previousURL + "," + arr[6] + "\n";

					writer.write(new_line);
					System.out.print(new_line);
				}
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