import java.beans.DesignMode;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
			new Table(tableName);
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

	public void updatePageList(String tableName, Vector<Page> pageList)
	{
		try {
			FileOutputStream f1 = new FileOutputStream("tables/"+tableName+"/PageList.class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(pageList);
			out.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
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
				throw new DBAppException("Wrong type");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		Vector<Page> pageList = deserializePageList("Student");

		if(pageList.isEmpty())
		{
			t.addPage();
			Vector<Hashtable<String, Object>> vec = deserialize(tableName, 1);
			vec.add(colNameValue);
			Page page = new Page("tables/" + tableName + "/Page" + 1 + ".class");
			page.min = colNameValue.get(primaryKey);
			page.max = colNameValue.get(primaryKey);
			pageList.add(page);
			serialize(tableName, 1, vec);
			updatePageList(tableName, pageList);

		}
		else
		{
			
				int i = (decidePage(pageList,colNameValue,primaryKey));
				
					Vector<Hashtable<String, Object>> page = deserializePage(pageList.get(i).path);
					if(page.size()>199) {
						Page page2 = new Page("tables/"+tableName+"/Page"+(i+2)+".class");
						t.addPage();
						
						
						Vector<Hashtable<String, Object>> vec = deserialize(tableName, t.getPageNum()-1);
						//vec.add(colNameValue);
						Vector<Vector<Hashtable<String, Object>>> temp = split(page,colNameValue,primaryKey);
						page2.min = temp.get(1).firstElement().get(primaryKey);
						page2.max = temp.get(1).lastElement().get(primaryKey);
						page2.size = temp.get(1).size();
						pageList.get(i).min = temp.get(0).firstElement().get(primaryKey);
						pageList.get(i).max = temp.get(0).lastElement().get(primaryKey);
						pageList.get(i).size = temp.get(0).size();
						pageList.add(i,page2);
						Page tempPage = pageList.get(i+1);
						pageList.remove(tempPage);
						pageList.add(i,tempPage);
						serialize(page2.path,temp.get(1));
						serialize(pageList.get(i).path, temp.get(0));
						t.updatePageList(pageList);
						
					}
					else {
						page.add(colNameValue);
						Vector<Hashtable<String, Object>> newPage = sortVector(page, primaryKey);
						pageList.get(i).min = newPage.firstElement().get(primaryKey);
						pageList.get(i).max = newPage.lastElement().get(primaryKey);
						pageList.get(i).size = newPage.size();
						serialize(pageList.get(i).path, newPage);
						t.updatePageList(pageList);
					}
				}
			
		
		
	}
	
	private int decidePage(Vector<Page> pageList, Hashtable<String, Object> colNameValue, String primaryKey) throws DBAppException {
		// TODO Auto-generated method stub
		int i = -1;
		for (int ii = 0; ii < pageList.size(); ii++) {
			if(Compare(colNameValue.get(primaryKey), pageList.get(ii).min))
				i++;
			else
				break;
		}
		
		return i;
	}

	public static Vector<Vector<Hashtable<String, Object>>>  split (Vector<Hashtable<String, Object>> page, Hashtable<String, Object> record, String primaryKey) throws DBAppException{
		page.add(record);

		page = sortVector(page, primaryKey);
		Vector<Vector<Hashtable<String,Object>>>  res = new Vector<Vector<Hashtable<String,Object>>>(2) ;
		res.add(new Vector<Hashtable<String, Object>>());
		res.add(new Vector<Hashtable<String, Object>>());
		for (int i = 0; i < page.size(); i++) {
			if(i<page.size()/2) {
				res.get(0).add(page.get(i));
			}
			else
				res.get(1).add(page.get(i));
			
		}
		return res;
	}
	
	
	
	
	
	public static Vector<Hashtable<String, Object>> sortVector(Vector<Hashtable<String, Object>> vec, Object primaryKey) throws DBAppException
	{
		boolean sorted = false;
	    Hashtable<String, Object> temp;
	    while(!sorted) {
	        sorted = true;
	        for (int i = 0; i < vec.size() - 1; i++) {
				if (Compare(vec.get(i).get(primaryKey), vec.get(i+1).get(primaryKey))) {
	                temp =  vec.get(i);
	                vec.remove(temp);
	                vec.add(i+1,temp);
	                sorted = false;
	            }
	        }
	    }
	    
	    return vec;
	}
	
	public String verifyColumnTypes(String tableName, Hashtable<String, Object> colNameValue, TableContent content)
			throws ClassNotFoundException, DBAppException, ParseException {
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
							if(!minmax(content.columns.get(i).min, content.columns.get(i).max, obj.toString(), content.columns.get(i).type))
								throw new DBAppException("value is out of range");
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

//	public boolean minMaxCheck(String min, String max, String value) {
//		return (min.compareToIgnoreCase(value) < 0) && (max.compareToIgnoreCase(value) > 0)
//				&& (min.length() <= value.length()) && (max.length() >= value.length());
//
//	}

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
	
	public static Vector<Hashtable<String, Object>> deserializePage(String path) {
		Vector<Hashtable<String, Object>> page = null;
		try {
			FileInputStream fileIn = new FileInputStream(path);
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
	
	@SuppressWarnings("unchecked")
	public static Vector<Page> deserializePageList(String tableName) {
		
		Vector<Page> pageList = null;
		try {
			FileInputStream fileIn = new FileInputStream("tables/" + tableName + "/PageList.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			pageList = (Vector<Page>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {

			c.printStackTrace();

		}
		return pageList;
		
		
	}
	
	public static void serialize(String path, Vector<Hashtable<String, Object>> v) {
		try {
			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(v);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
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
		Vector<Page> v = deserializePageList(tableName);
		for (int i = 0; i < v.size(); i++) {

			Vector<Hashtable<String, Object>> page = deserializePage(v.get(i).path);
			System.out.println(page.size());
			System.out.println(page.toString());
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

	public static boolean Compare(Object lastEntry, Object newEntry) throws DBAppException{
		if (newEntry instanceof String)
			return ((String) newEntry).compareToIgnoreCase((String) lastEntry) < 0;
		else if (newEntry instanceof Integer)
			return ((int) newEntry) < ((int) lastEntry);
		else if (newEntry instanceof Double)
			return new BigDecimal((double) newEntry).compareTo(new BigDecimal((double) lastEntry)) < 0;
		else if (newEntry instanceof Date)
			return ((Date) newEntry).compareTo((Date) lastEntry) < 0;
		else
			throw new DBAppException("Cannot typecast!!");

	}
public static boolean minmax(String min, String max, String value, String type) throws ClassNotFoundException, ParseException {
		boolean flag = false;
		Class x = Class.forName(type);
		if(x.getSimpleName().equals("Integer")) {
			Integer min2 = Integer.valueOf(min);
			Integer max2 = Integer.valueOf(max);
			Integer value2= Integer.valueOf(value);
			if(min2<=value2 && value2<=max2) {
				flag = true;
			}
		}
		if(x.getSimpleName().equals("Date")) {
			Date min2 = new SimpleDateFormat("yyyy-mm-dd").parse(min);
			Date max2 = new SimpleDateFormat("yyyy-mm-dd").parse(max);
			Date value2= new SimpleDateFormat("EEE MMM DD HH:mm:ss Z YYYY").parse(value);
			min2.setHours(0);
			min2.setSeconds(0);
			min2.setMinutes(0);
			max2.setHours(0);
			max2.setSeconds(0);
			max2.setMinutes(0);
			value2.setHours(0);
			value2.setSeconds(0);
			value2.setMinutes(0);
			if(value2.compareTo(min2)>=0&&value2.compareTo(max2)<=0) {
				flag = true;
			}
		}
		if(x.getSimpleName().equals("Double")) {
			Double min2 = Double.valueOf(min);
			Double max2 = Double.valueOf(max);
			Double value2= Double.valueOf(value);
			if(min2<=value2 && value2<=max2) {
				flag = true;
			}
		}
		if(x.getSimpleName().equals("String")) {
			if((min.length() <= value.length()) && (max.length() >= value.length())) {
				flag = true;
			}
			else if ((min.compareToIgnoreCase(value) <= 0) && (max.compareToIgnoreCase(value) >= 0))
				flag = true;
		}
		return flag;
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
		
		try {
			//dbApp.createTable("Student", "id", htblColNameType, htblColNameMin, htblColNameMax);
				Hashtable<String, Object> record = new Hashtable<String, Object>();
				record.put("gpa", 1.0);
				record.put("name", "name");
				record.put("id", 20000);
				dbApp.insertIntoTable("Student", record);
			
		
			
			printTable("Student");

		} catch (DBAppException e) {
			System.out.println(e.getMessage());

		}

	}

}