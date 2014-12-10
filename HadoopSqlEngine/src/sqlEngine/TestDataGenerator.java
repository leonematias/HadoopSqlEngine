package sqlEngine;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Tool to generate test data
 * 
 * @author Matias Leone
 */
public class TestDataGenerator {

	private final static Row[] USERS = new Row[] {
		new Row("0", "Matt Damon", "United States"),
		new Row("1", "Arnold Schwarzenegger", "Australia"),
		new Row("2", "Brad Pitt", "United States"),
		new Row("3", "Jack Nicholson", "Germany"),
		new Row("4", "Marlon Brando", "Italy"),
		new Row("5", "Robert De Niro", "United States"),
		new Row("6", "Al Pacino", "Germany"),
		new Row("7", "Daniel Day-Lewis", "United States"),
		new Row("8", "Dustin Hoffman", "Australia"),
		new Row("9", "Tom Hanks", "Italy"),
		new Row("10", "Anthony Hopkins", "Germany"),
		new Row("11", "Denzel Washington", "United States"),
		new Row("12", "Michael Caine", "Australia"),
		new Row("13", "Robin Williams", "United States"),
		new Row("14", "Robert Duvall", "Italy"),
		new Row("15", "Gene Hackman", "Italy"),
		new Row("16", "Morgan Freeman", "United States"),
		new Row("17", "Sean Penn", "Australia"),
		new Row("18", "Jeff Bridges", "Germany"),
		new Row("19", "Clint Eastwood", "United States"),
	};
	
	private final static Row[] PRODUCTS = new Row[] {
		new Row("0", "Grand Theft Auto V", "59.99"),
		new Row("1", "Dragon Age: Inquisition", "59.99"),
		new Row("2", "Minecraft: PlayStation 4 Edition", "29.89"),
		new Row("3", "Far Cry 4", "59.99"),
		new Row("4", "Middle-earth: Shadow of Mordor", "40.99"),
		new Row("5", "Call of Duty: Advanced Warfare", "59.99"),
		new Row("6", "NBA 2K15", "25.59"),
		new Row("7", "FIFA 15", "40.99"),
		new Row("8", "The Wolf Among Us", "25.59"),
		new Row("9", "Alien: Isolation", "40.99"),
		new Row("10", "Destiny", "40.99"),
		new Row("11", "LittleBigPlanet 3", "29.89"),
		new Row("12", "Watch Dogs: Bad Blood", "15.79"),
		new Row("13", "LEGO Batman 3: Beyond Gotham", "15.79"),
		new Row("14", "Assassin's Creed Unity", "59.99"),
		new Row("15", "Just Dance 2015", "25.59"),
		new Row("16", "NHL 15", "25.59"),
		new Row("17", "The Last of Us Remastered", "40.99"),
		new Row("18", "Diablo III: Ultimate Evil Edition", "29.89"),
		new Row("19", "Final Fantasy XIV Online: A Realm Reborn", "15.79"),
		new Row("20", "Battlefield 4", "15.79"),
		new Row("21", "Tomb Raider: Definitive Edition", "25.59"),
		new Row("22", "Assassin's Creed IV: Black Flag", "15.79"),
		new Row("23", "LEGO Marvel Super Heroes", "29.89"),
		new Row("24", "Madden NFL 15", "25.59"),
	};
	
	private final static Row[] STORES = new Row[] {
		new Row("0", "New York"),
		new Row("1", "Boston"),
		new Row("2", "Houston"),
		new Row("3", "Washington"),
		new Row("4", "Philadelphia"),
		new Row("5", "San Francisco"),
		new Row("6", "Miami"),
		new Row("7", "Chicago"),
		new Row("8", "Los Angeles"),
		new Row("9", "Phoenix"),
		new Row("10", "San Antonio"),
		new Row("11", "San Diego"),
		new Row("12", "Dallas"),
	};
	
	
	public static void main(String[] args) throws Exception {
		String basePath = args[0];
		int mainTableCount = Integer.parseInt(args[1]);
		
		new TestDataGenerator().generate(basePath, mainTableCount);
	}

	private void generate(String basePath, int mainTableCount) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		System.out.println("Creating test data in: " + basePath + " with " + mainTableCount + " rows.");
		
		//Create input dir
		Path inputDirPath = new Path(basePath);
		Utils.createNewDir(hdfs, inputDirPath);
		
		//Create reference tables
		createUsersTable(hdfs, inputDirPath);
		createProductsTable(hdfs, inputDirPath);
		createStoresTable(hdfs, inputDirPath);
		
		//Create main table
		createSalesTable(hdfs, inputDirPath, mainTableCount);
		
		hdfs.close();
		
		
		System.out.println("Data created successfully.");
	}

	

	private void createUsersTable(FileSystem hdfs, Path inputDirPath) throws Exception {
		Path dir = new Path(inputDirPath.toString() + "/user");
		hdfs.mkdirs(dir);
		
		Path file = new Path(dir.toString() + "/user.csv");
		BufferedWriter w = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));
		for (int i = 0; i < USERS.length; i++) {
			w.write(USERS[i].toString());
			if(i != USERS.length - 1)
				w.newLine();
		}
		w.close();
	}
	
	private void createProductsTable(FileSystem hdfs, Path inputDirPath) throws Exception {
		Path dir = new Path(inputDirPath.toString() + "/product");
		hdfs.mkdirs(dir);
		
		Path file = new Path(dir.toString() + "/product.csv");
		BufferedWriter w = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));
		for (int i = 0; i < PRODUCTS.length; i++) {
			w.write(PRODUCTS[i].toString());
			if(i != PRODUCTS.length - 1)
				w.newLine();
		}
		w.close();
	}
	
	private void createStoresTable(FileSystem hdfs, Path inputDirPath) throws Exception {
		Path dir = new Path(inputDirPath.toString() + "/store");
		hdfs.mkdirs(dir);
		
		Path file = new Path(dir.toString() + "/store.csv");
		BufferedWriter w = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));
		for (int i = 0; i < STORES.length; i++) {
			w.write(STORES[i].toString());
			if(i != STORES.length - 1)
				w.newLine();
		}
		w.close();
	}
	
	private void createSalesTable(FileSystem hdfs, Path inputDirPath, int mainTableCount) throws Exception {
		Path dir = new Path(inputDirPath.toString() + "/sale");
		hdfs.mkdirs(dir);
		
		Path file = new Path(dir.toString() + "/sale.csv");
		BufferedWriter w = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));
		Random r = new Random();
		for (int i = 0; i < mainTableCount; i++) {
			
			//userId,productId,storeId,quantity,day,hour
			Row row = new Row(
					USERS[r.nextInt(USERS.length)].get(0),
					PRODUCTS[r.nextInt(PRODUCTS.length)].get(0),
					STORES[r.nextInt(STORES.length)].get(0),
					String.valueOf(r.nextInt(5) + 1),
					String.valueOf(r.nextInt(31) + 1),
					String.valueOf(r.nextInt(24))
					);
			w.write(row.toString());
			if(i != mainTableCount - 1)
				w.newLine();
		}
		w.close();
	}
	
	
	private static class Row {
		private String[] values;
		public Row(String...values) {
			this.values = values;
		}
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
	    	for (int i = 0; i < values.length; i++) {
				sb.append(values[i]);
				if(i != values.length - 1) {
					sb.append(",");
				}
			}
	    	return sb.toString();
		}
		public String get(int i) {
			return values[i];
		}
	}
	
	
	
}
