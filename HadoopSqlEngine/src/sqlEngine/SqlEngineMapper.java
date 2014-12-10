package sqlEngine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import sqlEngine.SqlParser.ConditionalOperator;
import sqlEngine.SqlParser.Join;
import sqlEngine.SqlParser.JoinClause;
import sqlEngine.SqlParser.SelectColumn;
import sqlEngine.SqlParser.SelectColumnType;
import sqlEngine.SqlParser.WhereClause;
import sqlEngine.SqlParser.WhereCompoundClause;
import sqlEngine.SqlParser.WhereSimpleClause;

/**
 * Sql engine Mapper
 * 
 * @author Matias Leone
 */
public class SqlEngineMapper extends Mapper<Object, Text, Text, Text> {

	private final Text keyWritable = new Text();
	private final Text valueWritable = new Text();
	private List<CachedTable> tables;
	private SqlParser sql;
	private String columnSeparator;
	
    public void setup(Context context) throws IOException, InterruptedException {
    	//Parse sql
    	sql = new SqlParser(context.getConfiguration().get("sql"), false);
    	columnSeparator = context.getConfiguration().get("columnSeparator");
    	
    	//Load tables in memory
    	FileSystem hdfs = FileSystem.get(context.getConfiguration());
    	tables = new ArrayList<CachedTable>();
    	if(context.getCacheFiles() != null) {
    		for (URI uri : context.getCacheFiles()) {
        		tables.add(new CachedTable(hdfs, new Path(uri), columnSeparator));
        	}
    	}
    	
    	super.setup(context);
    }
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	//Current row of the main table
    	Record currentRow = new Record(value.toString(), columnSeparator);
    	
    	//Joins: search in cached tables and merge records into one
    	Map<String, Record> joinedRows = new HashMap<String, Record>();
    	joinedRows.put(sql.mainTable, currentRow);
    	for (Join join : sql.joins) {
    		CachedTable joinTable = getTable(join.table);
    		for (JoinClause joinClause : join.joinClauses) {
    			String currentRowValue = currentRow.getValue(joinClause.localColumn);
    			Record joinRow = joinTable.searchByColumn(joinClause.foreignColumn.column, currentRowValue);
    			
    			//Join not satisfied, abort and skip the current row
    			if(joinRow == null) {
    				return;
    			}
    			joinedRows.put(join.table, joinRow);
			}
		}
    	
    	
    	
    	//Where: apply filters
    	if(sql.whereClause != null) {

    		//Apply nested filters recursively
    		boolean result = applyWhereClauseRecursive(joinedRows, sql.whereClause);
    		
    		//Skip current line if the filters were not satisfied
    		if(!result) {
    			return;
    		}
    	}
    	
    	
    	//Select: leave only the columns that we want to see. Put them in reducer key
    	List<String> keyColumns = new ArrayList<String>();
    	for (SelectColumn selectColumn : sql.selectColumns) {
			//Constant: just add the value
			if(selectColumn.type == SelectColumnType.CONSTANT) {
				keyColumns.add(selectColumn.constant);
				
			//Regular column: add column value
			} else if(selectColumn.type == SelectColumnType.COLUMN) {
				String colValue = joinedRows.get(selectColumn.column.table).getValue(selectColumn.column.column);
				keyColumns.add(colValue);
			}
		}
    	
    	//Aggregation columns: put them in reducer value
    	List<String> valueColumns = new ArrayList<String>();
    	for (SelectColumn selectColumn : sql.selectColumns) {
    		if(selectColumn.type == SelectColumnType.AGGREGATE) {
    			String colValue = joinedRows.get(selectColumn.column.table).getValue(selectColumn.column.column);
    			valueColumns.add(colValue);
    		}
    	}
    	
    	
    	//Output concatenated columns in key and value
    	keyWritable.set(Utils.getColumsOutput(keyColumns, columnSeparator));
    	valueWritable.set(Utils.getColumsOutput(valueColumns, columnSeparator));
    	context.write(keyWritable, valueWritable);
    }
    
    /**
     * Apply WHERE filters recursively to joined rows
     */
    private boolean applyWhereClauseRecursive(Map<String, Record> joinedRows, WhereClause clause) {
    	
    	//Compound expression
    	if(clause instanceof WhereCompoundClause) {
    		WhereCompoundClause compClause = (WhereCompoundClause)clause;
    		
    		//Apply left clause first
    		boolean leftResult = applyWhereClauseRecursive(joinedRows, compClause.leftClause);
    		//Early abort if using an AND operator
    		if(!leftResult && compClause.operator == ConditionalOperator.AND)
    			return false;
    		
    		//Apply right clause
    		boolean rightResult = applyWhereClauseRecursive(joinedRows, compClause.rightClause);
    		
    		//Apply conditional operator
    		boolean result;
    		if(compClause.operator == ConditionalOperator.AND) {
    			result = leftResult && rightResult;
    		} else {
    			result = leftResult || rightResult;
    		}
    		return result;
    		
		//Final expression
		} else {
			//Get value to compare
			WhereSimpleClause simpleClause = (WhereSimpleClause)clause;
    		String colValue = joinedRows.get(simpleClause.column.table).getValue(simpleClause.column.column);
    		
    		//Apply operator
    		boolean result = false;
    		switch (simpleClause.operator) {
			case EQUALS:
				result = colValue.toLowerCase().equals(simpleClause.value);
				break;
			case NOT_EQUALS:
				result = !colValue.equals(simpleClause.value);
				break;
			case LIKE:
				result = colValue.toLowerCase().contains(simpleClause.value);
				break;
			case GREATER:
				result = colValue.compareTo(simpleClause.value) > 0;
				break;
			case GREATER_EQ:
				result = colValue.compareTo(simpleClause.value) >= 0;
				break;
			case LOWER:
				result = colValue.compareTo(simpleClause.value) < 0;
				break;
			case LOWER_EQ:
				result = colValue.compareTo(simpleClause.value) <= 0;
				break;
			}
    		
    		return result;
		}
    }
    
    
    
    private CachedTable getTable(String tableName) {
    	for (CachedTable t : tables) {
			if(t.table.equals(tableName))
				return t;
		}
    	throw new RuntimeException("Invalid table name: " + tableName);
    }
    

	private class CachedTable {
		public String table;
		public List<Record> rows;
		
		public CachedTable(FileSystem hdfs, Path path, String sep) {
			rows = new ArrayList<Record>();
			table = path.getName();
			table = table.substring(0, table.lastIndexOf('.'));
			
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(hdfs.open(path)));
	    		String line;
	    		while((line = reader.readLine()) != null) {
	    			rows.add(new Record(line, sep));
	    		}
			} catch (IOException e) {
				throw new RuntimeException("Error loading table in memory: " + path, e);
			} finally {
				try {
					if(reader != null) {
						reader.close();
					}
				} catch (IOException e) {
				}
			}
		}
	
		public Record searchByColumn(int column, String value) {
			for (Record row : rows) {
				if(row.getValue(column).equals(value))
					return row;
			}
			return null;
		}
	}
	
	
	
	
}
