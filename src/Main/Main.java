package Main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
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

	/**
	 * @param args
	 */
	private static final String DB_TABLE = "tmp/edrick_db/my_table";
	private static final int NO_RECORDS = 100000;
	private static String db_type_option;
	private static Database my_table;
	private static boolean exit = false;
	private static int selection;
	private static File file;

	public static void main(String[] args) throws DatabaseException, IOException {
		// Retrieve db type option
		db_type_option = args[0];
		
		// Open answers file
		file = new File("answers");
		// If file doesnt exists, then create it
		if(!file.exists()){
			file.createNewFile();
		}
		
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
			performSelection(selection);
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
			searchByKey();
			break;
		case 3:
			System.out.println("Retrieve Records with Given Data");
			if (db_type_option.equals("btree")) {
				searchByDataBTree();
			} else {
				searchByDataHash();
			}
			break;
		case 4:
			System.out.println("Retrieve Records with Given Range of Keys");
			if (db_type_option.equals("btree")) {
				searchByKeyRangeBTree();
				// TODO
			} else {
				searchByKeyRangeHash();
			}
			break;
		case 5:
			System.out.println("Destroying Database...");
			destroyDB();
			break;
		case 6:
			file.delete();
			exit=true;
			return;
		default:
			System.out.println("Invalid Selection\n");
			return;
		}

	}



	// Select 1: Create and Populate Database
	public static void createPopulateDB(String db_type) throws FileNotFoundException, DatabaseException {

		DatabaseConfig dbConfig = new DatabaseConfig();
		boolean input_err = true;
		while(input_err) {
			if (db_type.equals("btree")) {
				dbConfig.setType(DatabaseType.BTREE);
				input_err = false;
				break;
			} else if (db_type.equals("hash")) {
				dbConfig.setType(DatabaseType.HASH);
				input_err = false;
				break;
			} else if (db_type.equals("indexfile")) {
				System.out.println("Index File option not implemented yet");
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
//		String keyword = "upifbjzvdomrijhtvnmwyymfhglzhcsyxttdgjsqrzblznmireugvdamjcsvugqeyy";

		key.setData(keyword.getBytes());
		key.setSize(keyword.length());
		
		long startTime = System.nanoTime();
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
			//System.out.println("Key | Data : " + keyString + " | " + dataString + "");
			keyDataList.add(keyString);
			keyDataList.add(dataString);
			counter++;
			success = true;
		} else {
			success = false;
		}
		long endTime = System.nanoTime();
		// Gathers the elapsed time it took to search for the key
		long elapsedTime = (endTime - startTime)/1000000;
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
//		String dataword = "pmvndcccadcmvjijvcibttitcjvkrgtysvyrthbofxafnntddtgrehfudcyxybzlokplrturvzymryjshclxgryatxdotiainbpgzbynuyecxbqrvoq";
		DatabaseEntry givenData = new DatabaseEntry(dataword.getBytes());
		givenData.setSize(dataword.length());


		// Initialize cursor for table
		Cursor cursor = my_table.openCursor(null, null);

		ArrayList<String> keyDataList = new ArrayList<String>();
		int counter = 0;
		
		long startTime = System.nanoTime();

		while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		{
			String keyString = new String(key.getData());
			String dataString = new String(data.getData());
			//System.out.println("Key | Data : " + keyString + " | " + dataString + "");
			if (givenData.equals(data)) {
				keyDataList.add(keyString);
				keyDataList.add(dataString);
				counter++;
			}
		}
		long endTime = System.nanoTime();
		// Gathers the elapsed time it took to search for the data
		long elapsedTime = (endTime - startTime)/1000000;
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
	}

	public static void searchByDataHash() throws DatabaseException, IOException {

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		String dataword =  System.console().readLine("Enter Data value to be searched: ");
		System.console().printf("\n");
//		String dataword = "pmvndcccadcmvjijvcibttitcjvkrgtysvyrthbofxafnntddtgrehfudcyxybzlokplrturvzymryjshclxgryatxdotiainbpgzbynuyecxbqrvoq";
		DatabaseEntry givenData = new DatabaseEntry(dataword.getBytes());

		// Initialize cursor for table
		Cursor cursor = my_table.openCursor(null, null);

		ArrayList<String> keyDataList = new ArrayList<String>();
		int counter = 0;

		long startTime = System.nanoTime();

		while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		{
			String keyString = new String(key.getData());
			String dataString = new String(data.getData());
			System.out.println("Key | Data : " + keyString + " | " + dataString + "");
			if (givenData.equals(data)) {
				keyDataList.add(keyString);
				keyDataList.add(dataString);
				counter++;
			}
		}
		long endTime = System.nanoTime();
		// Gathers the elapsed time it took to search for the data
		long elapsedTime = (endTime - startTime)/1000000;
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
		//System.out.println("Test Counter: " + testcounter);
	}

	private static void sortHashDB() {
		
	}
	
	// TODO
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

		// Testing if min key exists
//		DatabaseEntry testKey = new DatabaseEntry();
//		testKey.setData("zyeaxpostikcxszddhnvhukukvxphqnhwyibkdcuiopepojyhjrppijhvowfwpiptmoenhbhnjvjzcnzwshonijsxdpawmdcvgzxvzxprhqgrmqizppytbaqnhwiufs".getBytes());
		
		System.out.println("MIN HASH CODE: " + minKey.getData().hashCode());
		System.out.println("MAX HASH CODE: " + maxKey.getData().hashCode());
//		System.out.println("TEST HASH CODE: " + testKey.getData().hashCode());

		long startTime = System.nanoTime();

		ArrayList<String> keyDataList = new ArrayList<String>();
		int counter = 0;
//		if (cursor.getSearchKeyRange(minKey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
//			String retKey = new String(minKey.getData());
//			String retData = new String(data.getData());
//			keyDataList.add(retKey);
//			keyDataList.add(retData);
//			System.out.println("Key | Data : " + retKey + " | " + retData + "");
//			counter++;
//			while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
//			{
//				String keyString = new String(key.getData());
//				String dataString = new String(data.getData());
//				keyDataList.add(keyString);
//				keyDataList.add(dataString);
//				System.out.println("Key | Data : " + keyString + " | " + dataString + "");
//				counter++;
//				if (new String(maxKey.getData()).equals(new String(key.getData()))) {
//					System.out.println("Reached MAX KEY");
//					break;
//				}
//				if (counter > 50) {
//					System.out.println("OVERFLOW!");
//					return counter;
//				}
//			}
//		}
		long endTime = System.nanoTime();
		// Gathers the elapsed time it took to search for the data
		long elapsedTime = (endTime - startTime)/1000000;
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
		//System.out.println("Test Counter: " + testcounter);
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

		// Testing if min key exists
		DatabaseEntry minData = new DatabaseEntry();
		if(my_table.get(null, minKey , minData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			System.out.println("Min Key Found!" + minKey.getData());

		}


		ArrayList<String> keyDataList = new ArrayList<String>();
		int counter = 0;
		
		long startTime = System.nanoTime();
		
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
				if (counter > 50) {
					System.out.println("OVERFLOW!");
					return counter;
				}
			}
		}
		long endTime = System.nanoTime();
		// Gathers the elapsed time it took to search for the data
		long elapsedTime = (endTime - startTime)/1000000;
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		printResult(file, keyDataList, elapsedTime);
		System.out.println("Total Records Retrieved: " + counter);
		return counter;

	}

	
	public static void printResult(File file, ArrayList<String> keyDataList, long elapsedTime) throws IOException {
		FileWriter fw = new FileWriter(file.getName(),true);
		BufferedWriter bw = new BufferedWriter(fw);

		int numRecords = keyDataList.size()/2;
		String columnNames = String.format("%-30s%-10s\n", "Number of Records Retrieved", "Total Execution Time");
		String values = String.format("%-30d%-10d ms\n", numRecords, elapsedTime);
		bw.write(columnNames);
		bw.write(values);
		for (int i = 0; i < numRecords; i++) {
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

	// Search Database by given Data (Deprecated)
	public static void searchByData() throws DatabaseException, IOException {
		long startTime = System.nanoTime();
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();

		// Initialize cursor for table
		Cursor cursor = my_table.openCursor(null, null);

		// Dataword to be searched
		String dataword = "bbmnfgntfghyxvcqyxfaquptpsjfbkxhbmieryrldlshglyocdrcvusmqmpcchkzoidslxqblghkyonajugpujoijhsupmo";

		data.setData(dataword.getBytes());
		data.setSize(dataword.length());

		int counter = 0;
		ArrayList<String> keyDataList = new ArrayList<String>();

		// Iterate over the entire database until all matches are found
		// It is possible for the database to have duplicate data values
		// Therefore, if dataword is found, store key/data pair into ArrayList and increment counter
		int testcounter = 0;
		while(cursor.getNext(key, data, LockMode.DEFAULT)==OperationStatus.SUCCESS) {
			String keyString = new String(key.getData());
			String dataString = new String(data.getData());
			System.out.println("Key | Data : " + keyString + " | " + dataString + "");
			if (dataString.equals(dataword)) {
				keyDataList.add(keyString);
				keyDataList.add(dataString);
				counter++;
			}
			testcounter++;

		}
		long endTime = System.nanoTime();
		// Gathers the elapsed time it took to search for the data
		long elapsedTime = (endTime - startTime)/1000000;
		System.out.println("Elapsed Time: " + elapsedTime + " ms");
		System.out.println("Total Count for Search By Data: " + counter);
		System.out.println("Test Counter: " + testcounter);
	}

}
