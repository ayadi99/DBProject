import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Vector;

public class DBApp implements DBAppInterface {

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		File tableCheck = new File("tables/" + tableName);
		if (tableCheck.exists()) {
			throw new DBAppException("a table with same name already exists");

		}

		try {
			@SuppressWarnings("unused")
			Table t1 = new Table(tableName);
			StringBuilder sb = new StringBuilder();
			Path pathToFile = Paths.get("src/main/resources/metadata.csv");
			BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII);
			String line = br.readLine();
			while (line != null) {
				sb.append(line + "\n");
				line = br.readLine();
			}
			// new content
			Enumeration<String> keysType = colNameType.keys();
			while (keysType.hasMoreElements()) {
				String key = keysType.nextElement();
				sb.append(tableName + ",");
				sb.append(key + ",");
				sb.append(colNameType.get(key) + ",");
				if (key.toString().equals(clusteringKey))
					sb.append("True,");
				else
					sb.append("False,");
				sb.append("False,");
				sb.append(colNameMin.get(key) + ",");
				sb.append(colNameMax.get(key) + "\n");
			}
			FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv");
			csvWriter.write(sb.toString());
			csvWriter.close();

		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		// TODO Auto-generated method stub

	}

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		if (!(new File("tables/" + tableName).exists()))
			throw new DBAppException("table doesnt exist");
		Table t = new Table(tableName);
		TableContent content = new TableContent(tableName);
		String primaryKey = "";
		try {
			primaryKey = verifyColumnTypes(tableName, colNameValue, content);
			if (primaryKey.equals("-1"))
				throw new DBAppException();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		boolean requireShift = false;
		int currentPage = 1;
		Hashtable<String, Object> row = null;
		Vector<Hashtable<String, Object>> page = null;
		int ii = 0;
		Hashtable<String, Object> temp = null;
		boolean found = false;
		for (int i = 1; i <= t.getPageNum(); i++) {
			page = deserialize(tableName, i);
			Collections.reverse(page);
			if (page.isEmpty())
				page.add(colNameValue);
			else
				for (ii = 0; ii < page.size() && ii < 200; ii++) {
					row = (Hashtable<String, Object>) page.get(ii);
					if (Compare(colNameValue.get(primaryKey), row.get(primaryKey))) {
						if (page.indexOf(page.lastElement()) >= 199) {
							break;
						}
						found = true;
						page.add(ii, colNameValue);
						break;
					}
				}
			Collections.reverse(page);
			serialize(tableName, i, page);
			if (found)
				break;
		}
		if (page.size() == 200) {
			t.addPage();
		}
		if (requireShift) {
			shiftAfterInsert(t, currentPage,temp);
		}

	}

	public String verifyColumnTypes(String tableName, Hashtable<String, Object> colNameValue, TableContent content)
			throws ClassNotFoundException, DBAppException {
		@SuppressWarnings("unused")
		String primaryKeyColumn = "";
		String key = "";
		Enumeration<String> keys = colNameValue.keys();
		while (keys.hasMoreElements()) {
			key = keys.nextElement();
			Object obj = colNameValue.get(key);
			// check types
			int i = 0;
			while (i < content.columns.size()) {
				if (key.equals(content.columns.get(i).name)) {
					if ((checkType(obj, content.columns.get(i).type))) {
//							if(!minMaxCheck(content.columns.get(i).min, content.columns.get(i).max, obj.toString()))
//								throw new DBAppException("value is out of range");
						if (content.columns.get(i).isCluster.equals("True")) {
							primaryKeyColumn = key;
						}
					} else
						return "-1";
				} else {
					i++;
					continue;
				}
				i++;
			}
		}
		return key;
	}

	public void shiftAfterInsert(Table t, int pageNum, Hashtable<String, Object> recordToShift) throws DBAppException {
		if (recordToShift == null)
			throw new DBAppException();
		if(t.getPageNum() == pageNum) {
			t.addPage();
			System.out.println("page");
		}
		Vector v;
		v = deserialize(t.TableName, pageNum);
		Collections.reverse(v);
		Hashtable<String, Object> tempTerm;
		if (v.size() == 200) {
			tempTerm = (Hashtable<String, Object>) v.lastElement();
			v.remove(v.firstElement());
			v.add(v.size(), recordToShift);
			//Collections.reverse(v);
			serialize(t.TableName, pageNum, v);
			shiftAfterInsert(t, pageNum + 1, tempTerm);
		} else {
			System.out.println("else");
			v.add(v.size(), recordToShift);
			Collections.reverse(v);
			serialize(t.TableName, pageNum, v);
		}

	}

	public boolean minMaxCheck(String min, String max, String value) {
		return (min.compareToIgnoreCase(value) < 0) && (max.compareToIgnoreCase(value) > 0)
				&& (min.length() <= value.length()) && (max.length() >= value.length());

	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		Table t = new Table(tableName);
		Vector<Hashtable<String, Object>> row;

		if (t.getPageNum() == 0 || t == null) { // if deleting from page with no rows or table with no pages
			throw new DBAppException();
		} else {
			for (int i = 1; i <= t.getPageNum(); i++) { // looping on each page
				row = deserialize(tableName, i);

				for (int j = 0; j < row.size(); j++) { // looping on each row ""check"
					TreeMap<String, Object> map = new TreeMap<String, Object>(row.get(j));
					if ((map).equals(columnNameValue)) { // find the key and the value
						if (row.size() == 1) { // if 1 vector with 1 record
							row.remove(j);
							t.deletePage(i);
						} else {
							row.remove(j);
						}
					}
					serialize(tableName, i, row);
				}
			}
		}

	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		// TODO Auto-generated method stub
		return null;
	}

	public static boolean checkType(Object obj, String c) throws ClassNotFoundException {
		return Class.forName(c).isInstance(obj);
	}

	@SuppressWarnings("unchecked")
	public static Vector<Hashtable<String, Object>> deserialize(String tableName, int pageNum) {
		Vector<Hashtable<String, Object>> page = null;
		try {
			FileInputStream fileIn = new FileInputStream("tables/" + tableName + "/Page" + pageNum + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			page = (Vector<Hashtable<String, Object>>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {

			c.printStackTrace();

		}
		return page;
	}

	public static void serialize(String tableName, int pageNum, Vector<Hashtable<String, Object>> v) {
		try {
			FileOutputStream fileOut = new FileOutputStream("tables/" + tableName + "/Page" + pageNum + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(v);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static void printTable(String tableName) throws DBAppException {
		Table t = new Table(tableName);

		int numOfPages = t.getPageNum();
		for (int i = 1; i <= numOfPages; i++) {

			Vector<Hashtable<String, Object>> page = deserialize(tableName, i);
			System.out.println(page.size());
			for (Object o : page) {
				@SuppressWarnings("unchecked")
				Hashtable<String, Object> arr = (Hashtable<String, Object>) o;

				Enumeration<Object> values = arr.elements();
				System.out.println("------");
				while (values.hasMoreElements()) {

					System.out.println(values.nextElement());
				}
			}
		}
	}

	public static void printPage(String tableName, int pageNum) {
		Vector<Hashtable<String, Object>> page = deserialize(tableName, pageNum);
		System.out.println(page.size());
		for (Object o : page) {
			@SuppressWarnings("unchecked")
			Hashtable<String, Object> arr = (Hashtable<String, Object>) o;

			Enumeration<Object> values = arr.elements();
			System.out.println("------");
			while (values.hasMoreElements()) {

				System.out.println(values.nextElement());
			}
		}
	}

	public static boolean Compare(Object lastEntry, Object newEntry) {
		if (newEntry instanceof String)
			return ((String) newEntry).compareToIgnoreCase((String) lastEntry) < 0;
		else if (newEntry instanceof Integer)
			return ((int) newEntry) < ((int) lastEntry);
		else if (newEntry instanceof Double)
			return new BigDecimal((double) newEntry).compareTo(new BigDecimal((double) lastEntry)) < 0;
		else
			return ((Date) newEntry).compareTo((Date) lastEntry) < 0;

	}

	public static void main(String a[]) throws Exception {

		DBApp dbApp = new DBApp();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");

		Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", "A");
		htblColNameMin.put("gpa", "0.7");

		Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
		htblColNameMax.put("id", "10000");
		htblColNameMax.put("name", "ZZZZZZZZZZZ");
		htblColNameMax.put("gpa", "12");

//		try {

//			dbApp.createTable("Student", "id", htblColNameType, htblColNameMin, htblColNameMax);
//			for (int i = 1; i <= 210; i++) {
//				if(i==112)
//					continue;
//				Hashtable<String, Object> record = new Hashtable<String, Object>();
//				record.put("gpa", 1.1);
//				record.put("name", "name");
//				record.put("id", i);
//				dbApp.insertIntoTable("Student", record);
//			}
//			Hashtable<String, Object> record = new Hashtable<String, Object>();
//			record.put("gpa", 1.1);
//			record.put("name", "name");
//			record.put("id", 112);
//			dbApp.insertIntoTable("Student", record);
//			printTable("Student");
//			
//		} catch (DBAppException e) {
//			System.out.println(e.getMessage());
//
//		}
		printPage("Student", 2);
		// dbApp.insertIntoTable("Student", null);
		// System.out.println(Compare(2, 1));
	}

}