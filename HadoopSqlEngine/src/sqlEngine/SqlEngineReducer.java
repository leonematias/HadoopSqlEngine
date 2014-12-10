package sqlEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import sqlEngine.SqlParser.AggregateFunction;
import sqlEngine.SqlParser.SelectColumn;
import sqlEngine.SqlParser.SelectColumnType;

/**
 * Sql engine Reducer
 * 
 * @author Matias Leone
 */
public class SqlEngineReducer extends Reducer<Text, Text, Text, Text> {

	private final static Text empty = new Text("");
	private final Text outputWritable = new Text();
	private SqlParser sql;
	private String columnSeparator;
	
	public void setup(Context context) throws IOException, InterruptedException {
		//Parse sql
    	sql = new SqlParser(context.getConfiguration().get("sql"), false);
    	columnSeparator = context.getConfiguration().get("columnSeparator");
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//Grouping: compute aggregate functions
		if(sql.requireGrouping) {
			
			//Parse grouping columns from key
			Record groupColumns = new Record(key.toString(), columnSeparator);
			
			//Init all aggregate values
			int count = 0;
			double sum = 0;
			double max = Double.MIN_VALUE;
			double min = Double.MAX_VALUE;
			
			//Loop through each value that we have to aggregate
			for (Text aggregateItem : values) {
				Record aggregateColums = new Record(aggregateItem.toString(), columnSeparator);
				
				//Loop trough aggregate columns
				int aggIndex = 0;
				for (SelectColumn c : sql.selectColumns) {
					if(c.type == SelectColumnType.AGGREGATE) {
						
						String colValue = aggregateColums.getValue(aggIndex);
						
						//Count
						if(c.function == AggregateFunction.COUNT || c.function == AggregateFunction.AVG) {
							count++;
						}
						
						//Sum
						if(c.function == AggregateFunction.SUM || c.function == AggregateFunction.AVG) {
							double v = Double.parseDouble(colValue);
							sum += v;
						}
						
						//Max and Min
						if(c.function == AggregateFunction.MAX || c.function == AggregateFunction.MIN) {
							double v = Double.parseDouble(colValue);
							if(v > max) max = v;
							if(v < min) min = v;
						}
						
						aggIndex++;
					}
				}
			}
			
			//Generate final list of columns
			List<String> resultColumns = new ArrayList<String>(sql.selectColumns.size());
			int groupColIndex = 0;
			for (SelectColumn c : sql.selectColumns) {
				
				//Add constant or column value
				if(c.type == SelectColumnType.CONSTANT || c.type == SelectColumnType.COLUMN) {
					resultColumns.add(groupColumns.getValue(groupColIndex));
					groupColIndex++;
					
				//Add aggregate calculation
				} else if(c.type == SelectColumnType.AGGREGATE) {
					switch (c.function) {
					case COUNT:
						resultColumns.add(String.valueOf(count));
						break;
					case SUM:
						resultColumns.add(Utils.printDouble(sum));
						break;
					case MAX:
						resultColumns.add(Utils.printDouble(max));
						break;
					case MIN:
						resultColumns.add(Utils.printDouble(min));
						break;
					case AVG:
						double avg = sum / (double)count;
						resultColumns.add(Utils.printDouble(avg));
						break;
					}
				}
			}

			outputWritable.set(Utils.getColumsOutput(resultColumns, columnSeparator));
			
			
		//No grouping: the key contains all the columns we need to output
		} else {
			outputWritable.set(key);
		}
		

		//We output everything in the key, the value is empty
		context.write(outputWritable, empty);
	}
	
}
