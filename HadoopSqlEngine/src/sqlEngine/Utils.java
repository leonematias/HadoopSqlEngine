package sqlEngine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Misc utilities
 * 
 * @author Matias Leone
 *
 */
public class Utils {

	private final static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#0.00");
	
	private Utils() {
	}
	
	public static String getColumsOutput(List<String> columns, String separator) {
    	StringBuilder sb = new StringBuilder();
    	for (int i = 0; i < columns.size(); i++) {
			sb.append(columns.get(i));
			if(i != columns.size() - 1) {
				sb.append(separator);
			}
		}
    	return sb.toString();
    }
	
	public static void createNewDir(FileSystem hdfs, Path path) {
		try {
			deleteDirRecursive(hdfs, path);
			hdfs.mkdirs(path);
		} catch (Exception e) {
			throw new RuntimeException("Error creating new dir in: " + path, e);
		}
	}
	
	public static void deleteDirRecursive(FileSystem hdfs, Path path) {
		try {
			if(!hdfs.exists(path))
				return;
			if(hdfs.isFile(path)) {
				hdfs.delete(path, true);
			} else {
				FileStatus[] fileStatus = hdfs.listStatus(path);
				for (FileStatus f : fileStatus) {
					deleteDirRecursive(hdfs, f.getPath());
				}
				hdfs.delete(path, true);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error deleting dir: " + path, e);
		}
		
	}
	
	public static List<Record> getReducerResults(FileSystem hdfs, Path reduceDir, String filePattern, String columnSeparator) {
		try {
			List<Record> results = new ArrayList<Record>();
			List<Path> files = Utils.getReducerFiles(hdfs, reduceDir, filePattern);
			for (Path file : files) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(file)));
	    		String line;
	    		while((line = reader.readLine()) != null) {
	    			results.add(new Record(line, columnSeparator));
	    		}
	    		reader.close();
			}
			return results;
		} catch (Exception e) {
			throw new RuntimeException("Error reading reducer files from: " + reduceDir, e);
		}
	}
	
	public static String getReducerResultsString(FileSystem hdfs, Path reduceDir, String filePattern, String columnSeparator) {	
		StringBuilder sb = new StringBuilder();
		List<Record> results = getReducerResults(hdfs, reduceDir, filePattern, columnSeparator);

		//Header
		if(results.size() > 0) {
			int n = results.get(0).getColumns().length;
			for (int i = 0; i < n; i++) {
				if(i != n - 1) {
					sb.append(padLeft("Column_" + (i + 1), 25, " "));
				} else {
					sb.append("Column_" + (i + 1));
				}
			}
			sb.append("\n---------------------------------------------------------------\n");
		}
		
		//Rows
		for (Record r : results) {
			String[] columns = r.getColumns();
			for (int i = 0; i < columns.length; i++) {
				if(i != columns.length - 1) {
					sb.append(padLeft(columns[i], 25, " "));	
				} else {
					sb.append(columns[i]);
				}
			}
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	public static void mergeReducerResults(FileSystem hdfs, Path reduceDir, String filePattern, Path mergeFile) {
		try {
			if(hdfs.exists(mergeFile)) {
				hdfs.delete(mergeFile, true);
			}
			BufferedWriter w = new BufferedWriter(new OutputStreamWriter(hdfs.create(mergeFile)));
			
			List<Path> files = getReducerFiles(hdfs, reduceDir, filePattern);
			for (Path file : files) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(file)));
	    		String line;
	    		while((line = reader.readLine()) != null) {
	    			w.append(line);
	    			w.newLine();
	    		}
	    		reader.close();
			}
			w.close();
		} catch (Exception e) {
			throw new RuntimeException("Error merging reducer files: " + filePattern + " in dir: " + reduceDir, e);
		}
	}
	
	public static List<Path> getReducerFiles(FileSystem hdfs, Path reduceDir, String filePattern) {
		try {
			List<Path> list = new ArrayList<Path>();
			FileStatus[] outputFiles = hdfs.listStatus(reduceDir);
			for (FileStatus fileStatus : outputFiles) {
				if(fileStatus.getPath().getName().contains(filePattern)) {
					list.add(fileStatus.getPath());
				}
			}
			return list;
		} catch (Exception e) {
			throw new RuntimeException("Error checking reducer output files inside: " + reduceDir + " with pattern: " + filePattern, e);
		}
		
	}
	
	public static void mergeFiles(FileSystem hdfs, Path srcDir, Path outputPath) {
		try {
			if(hdfs.exists(outputPath)) {
				hdfs.delete(outputPath, true);
			}
			BufferedWriter w = new BufferedWriter(new OutputStreamWriter(hdfs.create(outputPath)));
			
			FileStatus[] outputFiles = hdfs.listStatus(srcDir);
			for (FileStatus fileStatus : outputFiles) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(fileStatus.getPath())));
	    		String line;
	    		while((line = reader.readLine()) != null) {
	    			w.append(line);
	    			w.newLine();
	    		}
	    		reader.close();
			}
			w.close();
		} catch (Exception e) {
			throw new RuntimeException("Error merging files from dir: " + srcDir + " into file: " + outputPath, e);
		}
	}
	
	public static void moveReducerResults(FileSystem hdfs, Path reduceDir, String filePattern, Path dstDir) {
		try {
			FileStatus[] outputFiles = hdfs.listStatus(reduceDir);
			for (FileStatus fileStatus : outputFiles) {
				if(fileStatus.getPath().getName().contains(filePattern)) {
					Path dstPath = new Path(dstDir.toString() + "/" + fileStatus.getPath().getName());
					hdfs.rename(fileStatus.getPath(), dstPath);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Error moving reducer files: " + filePattern + " from dir: " + reduceDir + " to dir: " + dstDir, e);
		}
	}
	
	public static String reallAllFile(FileSystem hdfs, Path path) {
		try {
			StringBuilder sb = new StringBuilder();
			BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(path)));
    		String line;
    		while((line = reader.readLine()) != null) {
    			sb.append(line);
    			sb.append("\n");
    		}
    		reader.close();
    		return sb.toString();
		} catch (Exception e) {
			throw new RuntimeException("Error reading file content: " + path, e);
		}
	}
	
	
	public static String padLeft(String s, int n, String pad) {
		int c = n - s.length();
		StringBuilder sb = new StringBuilder(n);
		sb.append(s);
		for (int i = 0; i <= c; i++) {
			sb.append(pad);
		}
		return sb.toString();
	}
	
	public static String printDouble(double n) {
		return DECIMAL_FORMAT.format(n);
	}
	
}
