package sqlEngine;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop SQL engine
 * 
 * @author Matias Leone
 */
public class SqlEngine {

	public final static String COL_SEPARATOR = ",";
	public final static String REDURCER_FILES_PATTERN = "part-r-"; 
	
	private Path inputPath;
	private Path outputPath;
	private String columnSeparator;
	private FileSystem hdfs;
	
	/**
	 * Command-line entry-point
	 */
	public static void main(String[] args) throws Exception {
		SqlEngine engine = new SqlEngine();
		
		//Parse args
		String sql = null;
		boolean outputResults = false;
		for (int i = 0; i < args.length; i++) {
			if(args[i].equals("-input") && i < args.length - 1) {
				engine.setInputPath(new Path(args[i + 1]));
			} else if(args[i].equals("-output") && i < args.length - 1) {
				engine.setOutputPath(new Path(args[i + 1]));
			} else if(args[i].equals("-sep") && i < args.length - 1) {
				engine.setColumnSeparator(args[i + 1]);
			} else if(args[i].equals("-showResults")) {
				outputResults = true;
			} else if(args[i].equals("-sql") && i < args.length - 1) {
				sql = args[i + 1];
			}
		}
		if(sql == null) {
			System.err.println("sql not specified.");
			System.err.println("Usage:");
			System.err.println("$HADOOP_HOME/bin/hadoop jar HadoopSqlEngine.jar sqlEngine.SqlEngine -input home/input -output home/output -sep \",\" -showResults -sql \"SELECT user.1 FROM user\"");
			System.exit(1);
		}
		
		//Execute
		engine.executeQuery(sql);
		if(outputResults) {
			engine.displayResults();
		}
		
		//Dipose
		engine.close();
	}
	

	/**
	 * Creates new SqlEngine
	 */
	public SqlEngine() {
		columnSeparator = COL_SEPARATOR;
		inputPath = new Path("input");
		outputPath = new Path("output");
		
		//Connect to HDFS
		try {
			hdfs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Could not connect to HDFS", e);
		}
	}
	
	/**
	 * Execute the specified query
	 * @param sql query to be executed
	 * @return list of path to output reducer files with final results
	 */
	public List<Path> executeQuery(String sql) {
		
		System.out.println("=============================SqlEngine===============================");
		System.out.println("Executing with:");
		System.out.println("- "  + "input path: " + inputPath);
		System.out.println("- "  + "output path: " + outputPath);
		System.out.println("- "  + "column separator: " + columnSeparator);
		System.out.println("- "  + "sql: ");
		System.out.println(sql);
		System.out.println("=====================================================================");
		
		
		try {
			Configuration mainConf = new Configuration();
			final String basePath = inputPath.toString();
			
			//Parse sql
			mainConf.set("sql", sql);
			mainConf.set("columnSeparator", columnSeparator);
			SqlParser sqlParser = new SqlParser(sql, true);
			
			//Set main job
			Job mainJob = Job.getInstance(mainConf, "SqlEngine-Main");
			mainJob.setOutputKeyClass(Text.class);
			mainJob.setOutputValueClass(Text.class);
			mainJob.setJarByClass(SqlEngine.class);
			mainJob.setMapperClass(SqlEngineMapper.class);
			mainJob.setReducerClass(SqlEngineReducer.class);
			
			//Clean output dir
			if(hdfs.exists(outputPath)) {
				hdfs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(mainJob, outputPath);
			
			//Temp dir
			Path tmpDir = new Path("tmp");
			Utils.createNewDir(hdfs, tmpDir);
			
			//Add main table to input
			Path mainTablePath = new Path(basePath + "/" + sqlParser.mainTable);
			FileInputFormat.addInputPath(mainJob, mainTablePath);
			
			//Add secondary tables to distributed cache
			for (SqlParser.Join join : sqlParser.joins) {
				
				//Check if path exists
				Path joinTableDir = new Path(basePath + "/" + join.table + "/");
				if(!hdfs.exists(joinTableDir)) {
					throw new RuntimeException("Table: " + join.table + " does not exist in: " + joinTableDir.toString());
				}
				
				//Check how many files we have in that dir
				FileStatus[] fileStatus = hdfs.listStatus(joinTableDir);
				if(fileStatus.length == 0) {
					throw new RuntimeException("There is no file for table: " + join.table + " in dir: " + joinTableDir.toString());
				} else if(fileStatus.length == 1) {
					mainJob.addCacheFile(fileStatus[0].getPath().toUri());
				} else {
					//There are many files, merge them all in one temp file
					Path tmpFile = new Path(tmpDir.toString() + "/" + sqlParser.joins + ".tmp");
					Utils.mergeFiles(hdfs, joinTableDir, tmpFile);
					mainJob.addCacheFile(tmpFile.toUri());
				}
			}
			
			//Execute
			mainJob.waitForCompletion(true);
			
			//Clean temp dir
			Utils.deleteDirRecursive(hdfs, tmpDir);

			//Apply sorting phase
			if(sqlParser.sortClauses.size() > 0) {

				//Merge results into temp folder
				Utils.moveReducerResults(hdfs, outputPath, REDURCER_FILES_PATTERN, tmpDir);
				
				//Create sorting job 
				Configuration sortConf = new Configuration();
				sortConf.set("sql", sql);
				sortConf.set("columnSeparator", columnSeparator);
				Job sortJob = Job.getInstance(sortConf, "SqlEngine-Sorting");
				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				sortJob.setMapperClass(SortMapper.class);
				sortJob.setReducerClass(SortReducer.class);
				sortJob.setNumReduceTasks(1);
				FileInputFormat.addInputPath(sortJob, tmpDir);
				FileOutputFormat.setOutputPath(sortJob, outputPath);
				
				//Clean output dir
				if(hdfs.exists(outputPath)) {
					hdfs.delete(outputPath, true);
				}
				
				//Execute
				sortJob.waitForCompletion(true);
				
				//Clean temp dir
				Utils.deleteDirRecursive(hdfs, tmpDir);
			}
			
			
			//Return list of output files
			System.out.println("\n\n");
			List<Path> results = Utils.getReducerFiles(hdfs, outputPath, REDURCER_FILES_PATTERN);
			System.out.println(results.size() + " result files in: " + outputPath);
			return results;
			

		} catch (Exception e) {
			throw new RuntimeException("Error executing query: \n" + sql, e);
		}

	}
	
	/**
	 * Load results in memory and returns array of records.
	 * Must be executed after executeQuery().
	 * Results may not enter in memory.
	 * @return list of rows
	 */
	public List<Record> getResults() {
		return Utils.getReducerResults(hdfs, outputPath, REDURCER_FILES_PATTERN, columnSeparator);
	}
	
	/**
	 * Print results in stdout
	 */
	public void displayResults() {
		System.out.println("============================RESULTS=============================\n");
		String results = Utils.getReducerResultsString(hdfs, outputPath, REDURCER_FILES_PATTERN, columnSeparator);
		System.out.println(results);
		System.out.println("===============================================================");
	}
	
	/**
	 * Dispose resources
	 */
	public void close() {
		if(hdfs != null) {
			try {
				hdfs.close();
			} catch (IOException e) {
				throw new RuntimeException("Could not close HDFS", e);
			}
		}
	}
	


	public Path getInputPath() {
		return inputPath;
	}

	public void setInputPath(Path inputPath) {
		this.inputPath = inputPath;
	}

	public Path getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(Path outputPath) {
		this.outputPath = outputPath;
	}

	public String getColumnSeparator() {
		return columnSeparator;
	}

	public void setColumnSeparator(String columnSeparator) {
		this.columnSeparator = columnSeparator;
	}

	
	
	
	

}
