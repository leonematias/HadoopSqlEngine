package sqlEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple SQL parser
 * 
 * @author Matias Leone
 *
 */
public class SqlParser {

	public List<SelectColumn> selectColumns;
	public boolean distinct;
	public String mainTable;
	public List<Join> joins;
	public WhereClause whereClause;
	public List<Column> groupByColumns;
	public List<HavingColumn> havingColumns;
	public List<SortClause> sortClauses;
	public boolean requireGrouping;
	
	
	public static void main(String[] args) {
		String sql = "SELECT table1.1, table2.2, table3.1, COUNT(table1.1), SUM(table3.1) \n" +
				 "FROM table1 JOIN table2 ON table1.1 = table2.2 JOIN table3 ON table1.1 = table3.2 AND table1.2 = table3.2 \n" +
				 "WHERE table1.1 = '5' AND (table2.2 > '9' OR table3.1 like 'test') AND ((table1.2 = '7' OR (table1.2 <= '4' AND table1.3 != '20')) OR (table1.4 >= '20')) \n" +
				 "GROUP BY table1.1, table2.2, table3.1 \n" +
				 "ORDER BY 1 ASC, 2 DESC, 1";
		
		SqlParser parser = new SqlParser(sql, true);
		
		
		System.out.println(sql);
		System.out.println("--------");
		System.out.println(parser);
	}
	
	
	/**
	 * Parse SQL sentence
	 */
	public SqlParser(String sql, boolean validate) {
		sql = sql.toLowerCase();
		sql = sql.replace("\n", " ");
		sql = sql.replace("\t", " ");
		
		
		//Parse
		try {
			
			boolean hasWhere = sql.contains(" where");
			boolean hasGroupBy = sql.contains(" group by");
			boolean hasHaving = sql.contains(" having");
			boolean hasOrderBy = sql.contains(" order by");
			
			
			//Select
			selectColumns = new ArrayList<SelectColumn>();
			String selectContent = extractTextWithin(sql, "select ", " from");
			//Check if we have distinct
			distinct = false;
			if(selectContent.contains(" distinct ")) {
				distinct = true;
				selectContent = selectContent.substring(selectContent.indexOf(" distinct ") + " distinct ".length());
			}
			//Loop through columns
			requireGrouping = false;
			String[] selectSplit = selectContent.split(",");
			for (String col : selectSplit) {
				SelectColumn selectColumn = new SelectColumn();
				
				//Check if it's a constant
				if(col.trim().startsWith("'")) {
					selectColumn.type = SelectColumnType.CONSTANT;
					selectColumn.constant = col.substring(col.indexOf("'") + 1, col.lastIndexOf("'"));
					
				} else {
					//Check if it's an aggregate function
					AggregateFunction func = null;
					for (AggregateFunction f : AggregateFunction.values()) {
						if(col.trim().startsWith(f.name + "(")) {
							func = f;
							break;
						}
					}
					if(func == null) {
						//Regular table column
						selectColumn.type = SelectColumnType.COLUMN;
						selectColumn.column = new Column(col);
					} else {
						//Aggregate function
						requireGrouping = true;
						selectColumn.type = SelectColumnType.AGGREGATE;
						selectColumn.function = func;
						selectColumn.column = new Column(col.substring(col.indexOf("(") + 1, col.indexOf(")")));
					}
				}
				
				selectColumns.add(selectColumn);
			}
			
			
			//From
			String fromContext = "";
			//Extract from content
			if(hasWhere) {
				fromContext = extractTextWithin(sql, "from ", " where");
			} else if(hasGroupBy) {
				fromContext = extractTextWithin(sql, "from ", " group by");
			} else if(hasHaving) {
				fromContext = extractTextWithin(sql, "from ", " having");
			} else if(hasOrderBy) {
				fromContext = extractTextWithin(sql, "from ", " order by");
			} else {
				fromContext = extractLeftText(sql, "from ");
			}
			//With join
			joins = new ArrayList<Join>();
			if(fromContext.contains(" join ")) {
				String[] fromSplit = fromContext.split(" join ");
				mainTable = fromSplit[0].trim();
				for (int i = 1; i < fromSplit.length; i++) {
					String[] onSplit = fromSplit[i].split(" on ");

					//Table name
					Join join = new Join();
					join.table = onSplit[0].trim();
					join.joinClauses = new ArrayList<JoinClause>();
					
					//Clauses
					String[] clauses = onSplit[1].split(" and ");
					for (String clause : clauses) {
						String[] clauseSplit = clause.split("=");
						JoinClause joinClause = new JoinClause();
						
						//First column
						Column c1 = new Column(clauseSplit[0]);
						
						//Second column
						Column c2 = new Column(clauseSplit[1]);
						
						//One of the columns should belong to the main table and the other to the join table
						if(c1.table.equals(mainTable) && c2.table.equals(join.table)) {
							joinClause.localColumn = c1.column;
							joinClause.foreignColumn = c2;
						} else if(c2.table.equals(mainTable) && c1.table.equals(join.table)) {
							joinClause.localColumn = c2.column;
							joinClause.foreignColumn = c1;
						} else {
							throw new RuntimeException("Invalid join: " + clause);
						}
						
						join.joinClauses.add(joinClause);
					}
							
					joins.add(join);
				}
				
			//No join, only one table
			} else {
				mainTable = fromContext.trim();
			}
			
			
			//Where
			if(hasWhere) {
				//Extract where content
				String whereContext = "";
				if(hasGroupBy) {
					whereContext = extractTextWithin(sql, "where ", " group by");
				} else if(hasHaving) {
					whereContext = extractTextWithin(sql, "where ", " having");
				} else if(hasOrderBy) {
					whereContext = extractTextWithin(sql, "where ", " order by");
				} else {
					whereContext = extractLeftText(sql, "where ");
				}
				
				//Parse where clause recursive
				whereClause = parseWhereClauseRecursive(whereContext);
			}
			
			
			//Group by
			groupByColumns = new ArrayList<Column>();
			if(hasGroupBy) {
				//Extract group by content
				String groupByContent = "";
				if(hasHaving) {
					groupByContent = extractTextWithin(sql, "group by ", " having");
				} else if(hasOrderBy) {
					groupByContent = extractTextWithin(sql, "group by ", " order by");
				} else {
					groupByContent = extractLeftText(sql, "group by ");
				}
				String[] groupBySplit = groupByContent.split(",");
				for (String groupByStr : groupBySplit) {
					groupByColumns.add(new Column(groupByStr));
				}
			}
			
			//Having
			havingColumns = new ArrayList<HavingColumn>();
			if(hasHaving) {
				String havingContent = "";
				if(hasOrderBy) {
					havingContent = extractTextWithin(sql, "having ", " order by");
				} else {
					havingContent = extractLeftText(sql, "having ");
				}
				//TODO: complete HAVING parsing
			}
			
			//Order by
			sortClauses = new ArrayList<SortClause>();
			if(hasOrderBy) {
				String orderByContent = extractLeftText(sql, "order by ");
				String[] sortSplit = orderByContent.split(",");
				for (String sortStr : sortSplit) {
					SortClause sortClause = new SortClause();
					if(sortStr.contains(SortOrder.ASC.type)) {
						sortClause.order = SortOrder.ASC;
						sortClause.index = Integer.parseInt(sortStr.substring(0, sortStr.indexOf(" " + SortOrder.ASC.type)).trim());
					} else if(sortStr.contains(SortOrder.DESC.type)) {
						sortClause.order = SortOrder.DESC;
						sortClause.index = Integer.parseInt(sortStr.substring(0, sortStr.indexOf(" " + SortOrder.DESC.type)).trim());
					} else {
						//Assume ASC
						sortClause.order = SortOrder.ASC;
						sortClause.index = Integer.parseInt(sortStr.trim());
					}
					sortClauses.add(sortClause);
				}
			}
			
			
			
		} catch (Exception e) {
			throw new RuntimeException("Error parsing SQL: " + sql, e);
		}
		
		
		//Rum some validations on parsed values
		if(validate) {
			validate();
		}
		
	}
	
	/**
	 * Parse WHERE content recursive
	 */
	private WhereClause parseWhereClauseRecursive(String content) {
		//Content with parenthesis, extract outer most expression
		if(content.startsWith("(")) {
			
			//Find position of parenthesis that closes the current expression
			int openParenthesis = 1;
			int foundIndex = -1;
			for (int i = 1; i < content.length(); i++) {
				char c = content.charAt(i);
				if(c == '(') openParenthesis++;
				else if(c == ')') openParenthesis--;
				if(openParenthesis == 0) {
					foundIndex = i;
					break;
				}
			}
			if(foundIndex == -1) {
				throw new RuntimeException("Parenthesis are not well formed in where expression: " + content);
			}
			
			//Parse left clause recursive
			WhereClause leftClause = parseWhereClauseRecursive(content.substring(1, foundIndex).trim());
			
			//Search if we have more clauses on the right
			String rightContent = content.substring(foundIndex + 1);
			ConditionalOperator nextOp = getNextConditionalOperator(rightContent);
			
			//No more clauses
			if(nextOp == null) {
				return leftClause;
			}

			
			//Create compound clause
			WhereCompoundClause compoundClause = new WhereCompoundClause();
			compoundClause.leftClause = leftClause;
			compoundClause.operator = nextOp;
			
			//Parse right clause recursive
			int nextOpIndex = rightContent.indexOf(" " + nextOp.op + " ");
			compoundClause.rightClause = parseWhereClauseRecursive(rightContent.substring(nextOpIndex + nextOp.op.length() + 1).trim());
			
			return compoundClause;
			
		//Clean content, without parenthesis
		} else {
			
			//Search next conditional operator (if any)
			ConditionalOperator nextOp = getNextConditionalOperator(content);
			
			//Only one clause left
			if(nextOp == null) {
				return parseSimpleWhereClause(content);
				
			//More than one clause
			} else {
								
				//Create compound clause
				WhereCompoundClause compoundClause = new WhereCompoundClause();
				compoundClause.operator = nextOp;
				int nextOpIndex = content.indexOf(" " + nextOp.op + " ");
				
				//Left clause is a simple clause
				compoundClause.leftClause = parseSimpleWhereClause(content.substring(0, nextOpIndex).trim());
				
				//Parse right clause recursive
				compoundClause.rightClause = parseWhereClauseRecursive(content.substring(nextOpIndex + nextOp.op.length() + 1).trim());
				
				return compoundClause;
			}
		}
	}
	
	/**
	 * Parse simple WHERE expression
	 */
	private WhereSimpleClause parseSimpleWhereClause(String content) {
		//Search operator
		Operator operator = null;
		for (Operator op : Operator.values()) {
			if(content.contains(op.symbol)) {
				operator = op;
				break;
			}
		}
		if(operator == null) {
			throw new RuntimeException("Invalid operator in where clause: " + content);
		}
		
		//Create simple clause
		WhereSimpleClause whereClause = new WhereSimpleClause();
		whereClause.operator = operator;
		String[] operatorSplit = content.split(operator.symbol);
		String opLeft = operatorSplit[0].trim();
		String opRight = operatorSplit[1].trim();
		
		//Detect which side is the raw value with ''
		if(opLeft.charAt(0) == '\'') {
			whereClause.value = opLeft.replace("\'", "");
			whereClause.column = new Column(opRight);
		} else if(opRight.charAt(0) == '\'') {
			whereClause.value = opRight.replace("\'", "");
			whereClause.column = new Column(opLeft);
		} else {
			throw new RuntimeException("Invalid raw value in where clause: " + content);
		}
		
		return whereClause;
	}
	
	/**
	 * Find next Conditional Operator in a string
	 */
	private ConditionalOperator getNextConditionalOperator(String content) {
		//Search next conditional operator (if any)
		int andIndex = content.indexOf(" " + ConditionalOperator.AND.op + " ");
		int orIndex = content.indexOf(" " + ConditionalOperator.OR.op + " ");
		
		//Nothing found
		if(andIndex < 0 && orIndex < 0)
			return null;
		
		//Only OR
		if(andIndex < 0)
			return ConditionalOperator.OR;
		
		//Only AND
		if(orIndex < 0)
			return ConditionalOperator.AND;
		
		//Both present, return first one
		if(andIndex < orIndex)
			return ConditionalOperator.AND;
		return ConditionalOperator.OR;
	}
	
	
	
	/**
	 * Validate parsed values
	 */
	private void validate() {
		//Obtain all tables specified in FROM
		Map<String, Object> existingTables = new HashMap<String, Object>();
		existingTables.put(mainTable, null);
		for (Join j : joins) {
			existingTables.put(j.table, null);
		}
		
		//Check that all columns in the sentence are referencing a valid table specified in FROM
		for (SelectColumn c : selectColumns) {
			if(c.type == SelectColumnType.COLUMN || c.type == SelectColumnType.AGGREGATE) {
				if(!existingTables.containsKey(c.column.table)) {
					throw new RuntimeException("Select column use undefined table: " + c);
				}
			}
		}
		for (Join j : joins) {
			for (JoinClause joinClause : j.joinClauses) {
				if(!existingTables.containsKey(joinClause.foreignColumn.table)) {
					throw new RuntimeException("Join clause use undefined table: " + joinClause.foreignColumn.table);
				}
			}
		}
		if(whereClause != null) {
			validateWhereClause(existingTables, whereClause);
		}
		for (Column c : groupByColumns) {
			if(!existingTables.containsKey(c.table)) {
				throw new RuntimeException("Group By column use undefined table: " + c.table);
			}
		}
		
		//Check that all required groupBy columns are specified
		if(requireGrouping) {
			for (SelectColumn selCol : selectColumns) {
				if(selCol.type == SelectColumnType.COLUMN) {
					boolean found = false;
					for (Column groupByCol : groupByColumns) {
						if(groupByCol.table.equals(selCol.column.table) && groupByCol.column == selCol.column.column) {
							found = true;
							break;
						}
					}
					if(!found) {
						throw new RuntimeException("Column: " + selCol.column + " is not specified in Group By");
					}
				}
			}
		}
		
		//Check that sorting indices point to an existing select column
		for (SortClause s : sortClauses) {
			if(s.index < 0 || s.index >= selectColumns.size()) {
				throw new RuntimeException("Invalid Order By index: " + s.index);
			}
		}
		
		
	}
	
	private void validateWhereClause(Map<String, Object> existingTables, WhereClause clause) {
		if(clause instanceof WhereSimpleClause) {
			if(!existingTables.containsKey(((WhereSimpleClause) clause).column.table)) {
				throw new RuntimeException("Where clause use undefined table: " + clause);
			}
		} else {
			WhereCompoundClause compClause = (WhereCompoundClause)clause;
			validateWhereClause(existingTables, compClause.leftClause);
			validateWhereClause(existingTables, compClause.rightClause);
		}
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("SELECT ");
		for (int i = 0; i < selectColumns.size(); i++) {
			sb.append(selectColumns.get(i));
			if(i != selectColumns.size() - 1) {
				sb.append(", ");
			}
		}
		sb.append("\n");
		
		sb.append("FROM " + mainTable);
		for (Join j : joins) {
			sb.append(" JOIN " + j.table + " ON ");
			for (int i = 0; i < j.joinClauses.size(); i++) {
				sb.append(mainTable + "." + j.joinClauses.get(i));
				if(i != j.joinClauses.size() - 1) {
					sb.append(" AND ");
				}
			}
		}
		sb.append("\n");
		
		if(whereClause != null) {
			sb.append("WHERE ");
			sb.append(whereClause.toString());
			sb.append("\n");
		}
		
		if(groupByColumns.size() > 0) {
			sb.append("GROUP BY ");
			for (int i = 0; i < groupByColumns.size(); i++) {
				sb.append(groupByColumns.get(i));
				if(i != groupByColumns.size() - 1) {
					sb.append(", ");
				}
			}
			sb.append("\n");
		}
		
		if(sortClauses.size() > 0) {
			sb.append("ORDER BY ");
			for (int i = 0; i < sortClauses.size(); i++) {
				sb.append(sortClauses.get(i));
				if(i != sortClauses.size() - 1) {
					sb.append(", ");
				}
			}
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	private String extractTextWithin(String text, String start, String end) {
		return text.substring(text.indexOf(start) + start.length(), text.indexOf(end)).trim();
	}
	
	private String extractLeftText(String text, String start) {
		return text.substring(text.indexOf(start) + start.length()).trim();
	}
	
	
	public class Column {
		public String table;
		public int column;
		public Column(String s) {
			String[] split = s.split("\\.");
			if(split.length != 2) {
				throw new RuntimeException("Invalid table column: " + s);
			}
			this.table = split[0].trim();
			this.column = Integer.parseInt(split[1].trim());
		}
		@Override
		public String toString() {
			return table + "." + column;
		}
	}
	
	public enum SelectColumnType {
		CONSTANT,
		COLUMN,
		AGGREGATE
	}
	
	public class SelectColumn {
		public SelectColumnType type;
		public String constant;
		public Column column;
		public AggregateFunction function;
		@Override
		public String toString() {
			switch (type) {
			case CONSTANT:
				return "'" + constant + "'";
			case AGGREGATE:
				return function + "(" + column + ")";
			case COLUMN:
				return column.toString();
			default:
				return "";
			}
		}
	}
	
	public enum AggregateFunction {
		COUNT("count"),
		SUM("sum"),
		MAX("max"),
		MIN("min"),
		AVG("avg");
		public String name;
		private AggregateFunction(String name) {
			this.name = name;
		}
		@Override
		public String toString() {
			return name.toUpperCase();
		}
	}
	
	public class Join {
		public String table;
		List<JoinClause> joinClauses;
		@Override
		public String toString() {
			return table;
		}
	}
	
	public class JoinClause {
		public int localColumn;
		public Column foreignColumn;
		@Override
		public String toString() {
			return localColumn + " = " + foreignColumn;
		}
	}
	
	public enum Operator {
		NOT_EQUALS("!="),
		GREATER_EQ(">="),
		LOWER_EQ("<="),
		EQUALS("="),
		LIKE(" like "),
		GREATER(">"),
		LOWER("<");
		public final String symbol;
		private Operator(String s) {
			this.symbol = s;
		}
		@Override
		public String toString() {
			return symbol != LIKE.symbol ? " " + symbol + " " : symbol.toUpperCase();
		}
	}
		
	public enum ConditionalOperator {
		AND("and"),
		OR("or");
		public String op;
		private ConditionalOperator(String op) {
			this.op = op;
		}
		@Override
		public String toString() {
			return op.toUpperCase();
		}
	}
	
	public class WhereCompoundClause extends WhereClause {
		public WhereClause leftClause;
		public ConditionalOperator operator;
		public WhereClause rightClause;
		@Override
		public String toString() {
			return "(" + leftClause + " " + operator + " " + rightClause + ")";
		}
	}
	
	public class WhereSimpleClause extends WhereClause {
		public Column column;
		public Operator operator;
		public String value;
		@Override
		public String toString() {
			return column.toString() + operator + "'" + value + "'";
		}
	}
	
	public abstract class WhereClause {
	}
	
	public class HavingColumn {
		
	}
	
	public enum SortOrder {
		ASC("asc"),
		DESC("desc");
		public String type;
		private SortOrder(String type) {
			this.type = type;
		}
		@Override
		public String toString() {
			return type.toUpperCase();
		}
	}
	
	public class SortClause {
		public Integer index;
		public SortOrder order;
		@Override
		public String toString() {
			return index + " " + order;
		}
	}
	
	

	
}
