package org.example.processOutliers.util;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JobUtils {

	public static final String COUNTS = "counts";
	public static final String MEAN = "mean";
	public static final String ST_DEV = "stDev";
	public static final String FILTERED = "filtered";

	public static final String TAB_SEPARATOR = "\t";

	public static boolean isOutlier(Integer value, Double mean, Double stDev) {
		if (value < mean - 3*stDev || value > mean + 3*stDev) {
			return true;
		}
		return false;
	}

	public static void outputToCsv(FileSystem fs, Path outputDir, CSVWriter csvWriter, String separator) throws IOException {
		int fileCount = 0;

		while(true){
			Path outputFile = new Path(outputDir, String.format("part-r-%05d", fileCount));

			if(!fs.exists(outputFile)){
				break;
			}

			// Open results file
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outputFile)));
			try {
				// Loop through lines
				String line;
				while ((line = reader.readLine()) != null) {
					String[] row = line.split(separator);
					csvWriter.writeNext(row);
				}
			} finally {
				// Close and remove from CFS
				reader.close();
			}
			// Next batch
			fileCount++;
		}
	}
}
