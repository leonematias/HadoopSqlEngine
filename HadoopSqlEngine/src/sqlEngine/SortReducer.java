package sqlEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import sqlEngine.SqlParser.SortClause;
import sqlEngine.SqlParser.SortOrder;

/**
 * Reducer used in ORDER BY
 * 
 * @author Matias Leone
 */
public class SortReducer extends Reducer<IntWritable, Text, Text, Text> {

	private final static Text empty = new Text("");
	private final Text outputWritable = new Text();
	private SqlParser sql;
	private String columnSeparator;
	
	public void setup(Context context) throws IOException, InterruptedException {
		//Parse sql
    	sql = new SqlParser(context.getConfiguration().get("sql"), false);
    	columnSeparator = context.getConfiguration().get("columnSeparator");
	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Load all rows in memory
		List<Record> rows = new ArrayList<Record>();
		for (Text text : values) {
			rows.add(new Record(text.toString(), columnSeparator));
		}
		
		//Sort rows based on order by columns
		Collections.sort(rows, new Comparator<Record>() {
			@Override
			public int compare(Record a, Record b) {
				int result = 0;
				for (SortClause sortClause : sql.sortClauses) {
					String value1 = a.getValue(sortClause.index);
					String value2 = b.getValue(sortClause.index);
					
					result = value1.compareTo(value2);
					if(result != 0) {
						result = sortClause.order == SortOrder.ASC ? result : -result;
						return result;
					}
				}
				return result;
			}
		});
		
		//Output sorted rows
		for (Record row : rows) {
			outputWritable.set(row.toString());
			context.write(outputWritable, empty);
		}
		
	}
}


















