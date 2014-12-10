package sqlEngine;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper used in ORDER BY
 * 
 * @author Matias Leone
 */
public class SortMapper extends Mapper<Object, Text, IntWritable, Text> {

	private final static IntWritable one = new IntWritable(1);
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//Skip empty lines
		if(value.toString().trim().length() == 0)
			return;
		
		context.write(one, value);
	}
	
}
