package sqlEngine;


/**
 * A single row with its columns
 * 
 * @author Matias Leone
 */
public class Record {
	
	private int columnsCount;
	private String[] columns;

	/**
	 * Creates new record parsing one line
	 * @param line line to be parsed
	 * @param sep column separator
	 */
	public Record(String line, String sep) {
		String[] split = line.split(sep);
		columnsCount = split.length;
		columns = new String[columnsCount];
		for (int i = 0; i < split.length; i++) {
			columns[i] = split[i].trim();
		}
	}
	
	/**
	 * Get value of the specified index
	 */
	public String getValue(int colIndex) {
		if(colIndex < 0 || colIndex >= columns.length)
			throw new RuntimeException("Invalid column index: " + colIndex);
		return columns[colIndex];
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
    	for (int i = 0; i < columns.length; i++) {
			sb.append(columns[i]);
			if(i != columns.length - 1) {
				sb.append(SqlEngine.COL_SEPARATOR);
			}
		}
    	return sb.toString();
	}
	
	public String[] getColumns() {
		return columns;
	}
}
