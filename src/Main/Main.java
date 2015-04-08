package Main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;





public class Main {

	// TODO: Finish Report
	// TODO: Tabulate Results

	/**
	 * @param args
	 */
	private static final String DB_TABLE = "tmp/user_db/my_table";
	private static final String INV_TABLE = "tmp/user_db/inv_table";
	private static final int NO_RECORDS = 100000;
	private static String db_type_option;
	private static Database my_table;
	private static Database sec_table;
	private static boolean exit = false;
	private static int selection;
	private static File file;

	public static void main(String[] args) throws DatabaseException, IOException {
		// Retrieve db type option
		db_type_option = args[0];

		Main main = new Main();

		while(shouldNotExit()) {
			// Create Interface
			try {
				System.console().printf("\nMain Menu\n\n");
				selection = Integer.parseInt(System.console().readLine("1) Create and Populate Database\n" +
						"2) Retrieve Records with a given key\n" +
						"3) Retrieve Records with given data\n" +
						"4) Retrieve Records with a given range of keys\n" +
						"5) Destroy Database\n" +
						"6) Quit\n\nPlease Enter Selection Number: "));
			} catch (NumberFormatException e) {
				System.out.println("Invalid Input!\n");
				continue;
			}
			main.performSelection(selection);
		}
	}

	public Main() throws IOException {
		file = new File("answers");
		// If file doesnt exists, then create it
		if(!file.exists()){
			file.createNewFile();
		}
	}

	private static void performSelection(int selection) throws DatabaseException, IOException {
		switch(selection) {
			case 1:
				System.out.println("Creating Database...");
				createPopulateDB(db_type_option);
				System.out.println("");
				break;
			case 2:
				System.out.println("Retrieve Records with Given Key");
				if (db_type_option.equals("indexfile")) {
					searchByKeyIndexFile();
				} else {
					searchByKey();
				}
				break;
			case 3:
				System.out.println("Retrieve Records with Given Data");
				if (db_type_option.equals("btree")) {
					searchByDataBTree();
				} else if (db_type_option.equals("hash")){
					searchByDataHash();
				} else {
					// TODO INDEXFILE Data Value Search
					searchByDataIndexFile();
				}
				break;
			case 4:
				System.out.println("Retrieve Records with Given Range of Keys");
				if (db_type_option.equals("btree")) {
					searchByKeyRangeBTree();
					// TODO
				} else if (db_type_option.equals("hash")){
					searchByKeyRangeHash();
				} else {
					// TODO INDEXFILE Range Search
					searchByKeyRangeIndexFile();
				}
				break;
			case 5:
				System.out.println("Destroying Database...");
				destroyDB();
				break;
			case 6:
				exit=true;
				return;
			default:
				System.out.println("Invalid Selection\n");
				return;
		}

	}



	private static void searchByKeyRangeIndexFile()
	{

		// TODO Auto-generated method stub

	}

	private static void searchByDataIndexFile() throws DatabaseException, IOException
	{
		searchByDataBTree();
	}

	private static void searchByKeyIndexFile() throws DatabaseException, IOException
	{
		searchByKey();
	}

	// Select 1: Create and Populate Database
	public static void createPopulateDB(String db_type) throws FileNotFoundException, DatabaseException {

		DatabaseConfig dbConfig = new DatabaseConfig();
		boolean input_err = true;
		while(input_err) {
			if (db_type.equals("btree") || db_type.equals("indexfile")) {
				dbConfig.setType(DatabaseType.BTREE);
				input_err = false;
				break;
			} else if (db_type.equals("hash")) {
				dbConfig.setType(DatabaseType.HASH);
				input_err = false;
				break;
			} else {
				System.out.println("Invalid Input, try again...");
				continue;
			}
		}
		dbConfig.setAllowCreate(true);
		my_table = new Database(DB_TABLE, null, dbConfig);
		System.out.println(DB_TABLE + " has been created");
		System.out.println("Database Type: " + dbConfig.getType().toString());
		System.out.println();
		System.out.println("Populating database...");
		populateDB(my_table,NO_RECORDS);
		if(db_type.equals("indexfile")){
			sec_table=new Database(INV_TABLE, null, dbConfig);
			invertpopulateDB(sec_table, NO_RECORDS);
			
		}

	}

	// This populate database function is borrows from the provided java sample code
	// Reference: https://eclass.srv.ualberta.ca/pluginfile.php/1930632/mod_assign/intro/Sample.java
	private static void populateDB(Database my_table, int numRecords) {
		int range;
		DatabaseEntry kdbt, ddbt;
		String s;

		/*  
		 *  generate a random string with the length between 64 and 127,
		 *  inclusive.
		 *
		 *  Seed the random number once and once only.
		 */
		Random random = new Random(1000000);
		//System.out.println("Random Num: " + random);
		try {
			for (int i = 0; i < numRecords; i++) {

				/* to generate a key string */
				range = 64 + random.nextInt( 64 );
				s = "";
				for ( int j = 0; j < range; j++ ) 
					s+=(new Character((char)(97+random.nextInt(26)))).toString();
				//System.out.println("Key: " + s);

				/* to create a DBT for key */
				kdbt = new DatabaseEntry(s.getBytes());
				kdbt.setSize(s.length()); 

				// to print out the key/data pair
				// System.out.println(s);	
				/* to generate a data string */
				range = 64 + random.nextInt( 64 );
				s = "";
				for ( int j = 0; j < range; j++ ) 
					s+=(new Character((char)(97+random.nextInt(26)))).toString();
				// to print out tfw.close();he key/data pair
				//System.out.println("Data: " + s);	
				// System.out.println("");

				/* to create a DBT for data */
				ddbt = new DatabaseEntry(s.getBytes());
				ddbt.setSize(s.length()); 

				/* to insert the key/data pair into the database 
				 * if the key does not exist in the database already
				 */
				my_table.putNoOverwrite(null, kdbt, ddbt);
			}
		}
		catch (DatabaseException dbe) {
			System.err.println("Populate the table: "+dbe.toString());
			System.exit(1);
		}

	}
	private static void invertpopulateDB(Database my_table, int numRecords) {
		int range;
		DatabaseEntry kdbt, ddbt;
		String s;

		/*  
		 *  generate a random string with the length between 64 and 127,
		 *  inclusive.
		 *
		 *  Seed the random number once and once only.
		 */
		Random random = new Random(1000000);
		//System.out.println("Random Num: " + random);
		try {
			for (int i = 0; i < numRecords; i++) {

				/* to generate a key string */
				range = 64 + random.nextInt( 64 );
				s = "";
				for ( int j = 0; j < range; j++ ) 
					s+=(new Character((char)(97+random.nextInt(26)))).toString();
				//System.out.println("Key: " + s);

				/* to create a DBT for key */
				kdbt = new DatabaseEntry(s.getBytes());
				kdbt.setSize(s.length()); 

				// to print out the key/data pair
				// System.out.println(s);	
				/* to generate a data string */
				range = 64 + random.nextInt( 64 );
				s = "";
				for ( int j = 0; j < range; j++ ) 
					s+=(new Character((char)(97+random.nextInt(26)))).toString();
				// to print out tfw.close();he key/data pair
				//System.out.println("Data: " + s);	
				// System.out.println("");

				/* to create a DBT for data */
				ddbt = new DatabaseEntry(s.getBytes());
				ddbt.setSize(s.length()); 

				/* to insert the key/data pair into the database 
				 * if the key does not exist in the database already
				 */
				my_table.putNoOverwrite(null,ddbt,kdbt);
			}
		}
		catch (DatabaseException dbe) {
			System.err.println("Populate the table: "+dbe.toString());
			System.exit(1);
		}

	}


	public static void destroyDB() throws FileNotFoundException, DatabaseException {
		/* close the database and the database environment */
		my_table.close();
		/* to remove the table */
		my_table.remove(DB_TABLE,null,null);
	}


	// Search the Database using a Key
	public static boolean searchByKey() throws DatabaseException, IOException {
		boolean success = true;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		// Search for Keyword
		String keyword =  System.console().readLine("Enter Key value to be searched: ");
		System.console().printf("\n");
		// String keyword = "upifbjzvdomrijhtvnmwyymfhglzhcsyxttdgjsqrzblznmireugvdamjcsvugqeyy";

		key.setData(keyword.getBytes());
		key.setSize(keyword.length());
		
		// Start Timer
		long startTime = System.currentTimeMillis();

		OperationStatus op_status = my_table.get(null, key, data, LockMode.DEFAULT);
		System.out.println("Search Status: " + op_status.toString());


		op_status = my_table.get(null, key, data, LockMode.DEFAULT);

		int counter = 0;
		// Enters the matched key/data pair to an ArrayList to prepare for printing
		// Also return a boolean for unit testing
		ArrayList<String> keyDataList = new ArrayList<String>();
		
		if (op_status == OperationStatus.SUCCESS) {
			String keyString = new String(key.getData());
			String dataString = new String(data.getData());
			// System.out.println("Key | Data : " + keyString + " | " + dataString + "");
			keyDataList.add(keyString);
			keyDataList.add(dataString);
			counter++;
			success = true;
		} else {
			success = false;
		}
		
		// End Timer
		long endTime = System.currentTimeMillis();
		long elapsedTime = (endTime - startTime);
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
		return success;

	}


	public static void searchByDataBTree() throws DatabaseException, IOException {

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		String dataword =  System.console().readLine("Enter Data value to be searched: ");
		System.console().printf("\n");
		
		// String dataword = "pmvndcccadcmvjijvcibttitcjvkrgtysvyrthbofxafnntddtgrehfudcyxybzlokplrturvzymryjshclxgryatxdotiainbpgzbynuyecxbqrvoq";
		
		DatabaseEntry givenData = new DatabaseEntry(dataword.getBytes());
		givenData.setSize(dataword.length());


		// Initialize cursor for table
		Cursor cursor = my_table.openCursor(null, null);

		ArrayList<String> keyDataList = new ArrayList<String>();
		int counter = 0;

		// Start Timer
		long startTime = System.currentTimeMillis();

		while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		{
			String keyString = new String(key.getData());
			String dataString = new String(data.getData());
			if (givenData.equals(data)) {
				// System.out.println("Key | Data : " + keyString + " | " + dataString + "");
				keyDataList.add(keyString);
				keyDataList.add(dataString);
				counter++;
			}
		}
		
		// End Timer
		long endTime = System.currentTimeMillis();
		long elapsedTime = (endTime - startTime);
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
	}

	public static void searchByDataHash() throws DatabaseException, IOException {

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		String dataword =  System.console().readLine("Enter Data value to be searched: ");
		System.console().printf("\n");
		
		// String dataword = "pmvndcccadcmvjijvcibttitcjvkrgtysvyrthbofxafnntddtgrehfudcyxybzlokplrturvzymryjshclxgryatxdotiainbpgzbynuyecxbqrvoq";
		
		DatabaseEntry givenData = new DatabaseEntry(dataword.getBytes());

		// Initialize cursor for table
		Cursor cursor = my_table.openCursor(null, null);

		ArrayList<String> keyDataList = new ArrayList<String>();
		int counter = 0;

		// Start Timer
		long startTime = System.currentTimeMillis();

		while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		{
			String keyString = new String(key.getData());
			String dataString = new String(data.getData());
			if (givenData.equals(data)) {
				//System.out.println("Key | Data : " + keyString + " | " + dataString + "");
				keyDataList.add(keyString);
				keyDataList.add(dataString);
				counter++;
			}
		}
		
		// End Timer
		long endTime = System.currentTimeMillis();
		long elapsedTime = (endTime - startTime);
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
	}


	public static int searchByKeyRangeHash() throws DatabaseException, IOException {

		DatabaseEntry minKey = new DatabaseEntry();
		DatabaseEntry maxKey = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		Cursor cursor = my_table.openCursor(null, null);

		String minKeyword =  System.console().readLine("Enter minimum key: ");
		System.console().printf("\n");
		String maxKeyword =  System.console().readLine("Enter maximum key: ");
		System.console().printf("\n");

//		String minKeyword = "zydzqjcmmuklumwqehbphtpcubnoedzepzsgpivhlivbstrxyirjyfjmjbwkzaprlanyvvbtkztqmhdgjnudwnfaoivomxbzoajhmljejbxlwtqizppytbaqnhwiufs";
//		String maxKeyword = "zyhfkxxoyezbprhyvpqtuocjhxunskhioctskyaacafhxdarseypgbzdmxyehqkpnedxgtsditwndxsqdbiahzxwmdgvhofaavgmezeyqjszvskmgnyafqpzubqafso";

		minKey.setData(minKeyword.getBytes());
		minKey.setSize(minKeyword.length());

		maxKey.setData(maxKeyword.getBytes());
		maxKey.setSize(maxKeyword.length());


		ArrayList<String> keyDataList = new ArrayList<String>();
		int counter = 0;

		// Start Timer
		long startTime = System.currentTimeMillis();

		while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			String keyString = new String(key.getData());
			String dataString = new String(data.getData());

			if (minKeyword.compareTo(keyString) < 0 & maxKeyword.compareTo(keyString) > 0) {
				keyDataList.add(keyString);
				keyDataList.add(dataString);
				System.out.println("Key | Data : " + keyString + " | " + dataString + "");
				counter++;
			}	
		}
		
		// End Timer
		long endTime = System.currentTimeMillis();
		long elapsedTime = (endTime - startTime);
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
		return counter;
	}

	
	public static int searchByKeyRangeBTree() throws DatabaseException, IOException {

		DatabaseEntry minKey = new DatabaseEntry();
		DatabaseEntry maxKey = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		Cursor cursor = my_table.openCursor(null, null);

		String minKeyword =  System.console().readLine("Enter minimum key: ");
		System.console().printf("\n");
		String maxKeyword =  System.console().readLine("Enter maximum key: ");
		System.console().printf("\n");
		
//		String minKeyword = "zydzqjcmmuklumwqehbphtpcubnoedzepzsgpivhlivbstrxyirjyfjmjbwkzaprlanyvvbtkztqmhdgjnudwnfaoivomxbzoajhmljejbxlwtqizppytbaqnhwiufs";
//		String maxKeyword = "zyhfkxxoyezbprhyvpqtuocjhxunskhioctskyaacafhxdarseypgbzdmxyehqkpnedxgtsditwndxsqdbiahzxwmdgvhofaavgmezeyqjszvskmgnyafqpzubqafso";

		minKey.setData(minKeyword.getBytes());
		minKey.setSize(minKeyword.length());

		maxKey.setData(maxKeyword.getBytes());
		maxKey.setSize(maxKeyword.length());

		ArrayList<String> keyDataList = new ArrayList<String>();
		int counter = 0;

		// Start Timer
		long startTime = System.currentTimeMillis();

		if (cursor.getSearchKeyRange(minKey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			String retKey = new String(minKey.getData());
			String retData = new String(data.getData());
			keyDataList.add(retKey);
			keyDataList.add(retData);
			System.out.println("Key | Data : " + retKey + " | " + retData + "");
			counter++;
			while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
			{
				String keyString = new String(key.getData());
				String dataString = new String(data.getData());
				keyDataList.add(keyString);
				keyDataList.add(dataString);
				System.out.println("Key | Data : " + keyString + " | " + dataString + "");
				counter++;
				if (new String(maxKey.getData()).equals(new String(key.getData()))) {
					System.out.println("Reached MAX KEY");
					break;
				}
				if (counter > NO_RECORDS) {
					System.out.println("OVERFLOW!");
					return counter;
				}
			}
		}
		
		// End Timer
		long endTime = System.currentTimeMillis();
		long elapsedTime = (endTime - startTime);
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
		return counter;
	}


	public static void printResult(File file, ArrayList<String> keyDataList, long elapsedTime) throws IOException {
		FileWriter fw = new FileWriter(file.getName(),true);
		BufferedWriter bw = new BufferedWriter(fw);

		int numRecords = keyDataList.size();
		String columnNames = String.format("%-30s%-10s\n", "Number of Records Retrieved", "Total Execution Time");
		String values = String.format("%-30d%-10d ms\n", numRecords/2, elapsedTime);
		bw.write(columnNames);
		bw.write(values);
		for (int i = 0; i < numRecords; i = i + 2) {
			bw.write(keyDataList.get(i).toString());
			bw.newLine();
			bw.write(keyDataList.get(i+1).toString());
			bw.newLine();
			bw.newLine();
		}
		bw.close();
	}

	
	private static boolean shouldNotExit() {
		return !exit;
	}
}
