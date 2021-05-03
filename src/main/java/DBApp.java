import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;



public class DBApp implements DBAppInterface {
	int maxPageSize;
	final static String relativePath = "src/main/resources/data/";
	@Override
	public void init() {
		File ParentFile = new File(relativePath);
		
		if(!ParentFile.exists()) {
			ParentFile.mkdir();
		}
		Properties prop = new Properties();
        try {
            // the configuration file name
            String fileName = "DBApp.config";
            ClassLoader classLoader = DBApp.class.getClassLoader();

            // Make sure that the configuration file exists
            URL res = Objects.requireNonNull(classLoader.getResource(fileName),
                "Can't find configuration file DBApp.config");

            InputStream is = new FileInputStream(res.getFile());

            // load the properties file
            prop.load(is);

            // get the value for app.name key
            maxPageSize = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));

        } catch (IOException e) {
            e.printStackTrace();
        }

	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		File tableCheck = new File(relativePath + tableName);
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
	

	}

	public void updatePageList(String tableName, Vector<Page> pageList)
	{
		try {
			FileOutputStream f1 = new FileOutputStream(relativePath+tableName+"/PageList.class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(pageList);
			out.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		if (!(new File(relativePath + tableName).exists()))
			throw new DBAppException("table doesnt exist");
		Table t = new Table(tableName);
		TableContent content = new TableContent(tableName);
		String primaryKey = "";
		try {
			if(colNameValue.size() != content.columns.size())
				throw new DBAppException("missing attribute");
			primaryKey = verifyColumnTypes(tableName, colNameValue, content);
			if (primaryKey.equals("-1")) {
				throw new DBAppException("Wrong type");
			}
				
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		

		Vector<Page> pageList = deserializePageList(tableName);

		if(pageList.isEmpty())
		{
			t.addPage();
			Vector<Hashtable<String, Object>> vec = deserialize(tableName, 1);
			vec.add(colNameValue);
			Page page = new Page(relativePath + tableName + "/Page" + 1 + ".class");
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
					if(page.size()>maxPageSize-1) {
						Page page2 = new Page(relativePath+tableName+"/Page"+(i+2)+".class");
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
		int i = 0;
		for (int ii = 0; ii < pageList.size()-1; ii++) {
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

			int i = 0;
			while (i < content.columns.size()) {
				if (key.equals(content.columns.get(i).name)) {
					if ((checkType(obj, content.columns.get(i).type))) {
							if(!minmax(content.columns.get(i).min, content.columns.get(i).max, obj.toString(), content.columns.get(i).type))
								throw new DBAppException("The value is out of range");

						if ((Boolean.parseBoolean(content.columns.get(i).isCluster.toLowerCase()))) {
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

		return primaryKeyColumn;
	}


public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
		

		if(clusteringKeyValue.charAt(0) == '0')
			clusteringKeyValue = Integer.parseInt(clusteringKeyValue) + "";
		
		Table tableReferance = new Table(tableName);
		TableContent TC = new TableContent(tableName);
		Vector<Page> pageList = deserializePageList(tableName);
	
			boolean pKeyNotFound = true;
			
			for(int pageIndex = 0; pageIndex < pageList.size(); pageIndex++)
			{
		        Vector<Hashtable<String, Object>> vec = deserializePage(pageList.get(pageIndex).path);
		     	        
		        for(int  i = 0 ; i < vec.size(); i++)
		        {
		        	Hashtable<String,Object> arrHtbl = vec.get(i);
		        	boolean flag = false;
		        	
		        			      		        	
		        	Enumeration<String> keys1  = arrHtbl.keys();
		        	while(keys1.hasMoreElements())
		        	{
		        				String value;
		        	            String key = keys1.nextElement();
		        	            if(arrHtbl.get(key) instanceof Date)
		        	            {		        	            	
		        	            	value = dateToString((Date)arrHtbl.get(key));
		        	            }		        	            
		        	            	
		        	            else
		        	            	value = arrHtbl.get(key) + ""; //WE CHANGED THIS FROM STRING TO OBJECT
		        	            
		        	            
		        	            
		        	            if(key.equals(TC.getPrimaryKey(tableName)) && (value.equals(clusteringKeyValue)))
		        	            {	
		        	            	
		        	            	flag = true;
		        	            	pKeyNotFound = false;
				        			break;
		        	            }
		        	}
		        	
		        	Enumeration<String> keys2  = arrHtbl.keys();
		        	
		        	if(flag)
		        	{		       			        	
	        	         Enumeration<String> keys  = columnNameValue.keys();
	        	         
	        	         while(keys.hasMoreElements())
	        	         {
	        	        	 String key = keys.nextElement();
	        	        	 if(!arrHtbl.containsKey(key))
	        	        	 {
	        	        		 throw new DBAppException("Invalid Column Name");
	        	        	 }
			        		 Object value = columnNameValue.get(key);
		        	         arrHtbl.replace(key, value);

	        	         }

	        	         serialize(pageList.get(pageIndex).path,vec);
		        	}		        	
			     
	           }
		    }
			
			if(pKeyNotFound)
				throw new DBAppException("Record not found");
			
			}
			


	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		Table t = new Table(tableName);
		Vector<Hashtable<String, Object>> page;
		Vector<Page> pageList = deserializePageList(tableName);
		TableContent content = new TableContent(tableName);
		Set<Entry<String, Object>> record = columnNameValue.entrySet();
		
		boolean removed = false;
		if (!(new File(relativePath + tableName).exists()))
			throw new DBAppException("table doesnt exist");
		String primaryKey = "";
		try {
			primaryKey = verifyColumnTypes(tableName, columnNameValue, content);
			
		} catch (ClassNotFoundException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(primaryKey.equals("-1"))
			throw new DBAppException("Type mismatch");
		
		if (pageList.isEmpty()) { // if deleting from page with no rows or table with no pages
			throw new DBAppException("Table is empty");
		} else {
			for (int i = 0; i < pageList.size(); i++) { // looping on each page
				
				if(i>= pageList.size()) {
					throw new DBAppException("record not in table");
				}
				
				page = deserializePage(pageList.get(i).path);
				
				for (int j = 0; j < page.size(); j++) 
				{ 
					Set<Entry<String, Object>> row = page.get(j).entrySet();
					if(row.containsAll(record))
					{
						removed = true;
						page.remove(j);
						if(page.isEmpty())
						{
							t.deletePage(pageList.get(i).path);
							pageList.remove(i);
							t.updatePageList(pageList);
						}
						else
							serialize(pageList.get(i).path, page);
					}
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
			FileInputStream fileIn = new FileInputStream( relativePath+ tableName + "/Page" + pageNum + ".class");
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
			FileInputStream fileIn = new FileInputStream(relativePath + tableName + "/PageList.class");
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
			FileOutputStream fileOut = new FileOutputStream(relativePath + tableName + "/Page" + pageNum + ".class");
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

	public static boolean Compare(Object lastEntry, Object newEntry) throws DBAppException {
		int res;
		if (newEntry instanceof String) {
			res = ((String) newEntry).compareToIgnoreCase((String) lastEntry);
			if (res == 0)
				throw new DBAppException("primary key already in database");
			return res < 0;
		} else if (newEntry instanceof Integer) {
			if (((int) newEntry) == ((int) lastEntry))
				throw new DBAppException("primary key already in database");
			return ((int) newEntry) < ((int) lastEntry);
		} else if (newEntry instanceof Double) {
			res = new BigDecimal((double) newEntry).compareTo(new BigDecimal((double) lastEntry));
			if (res == 0)
				throw new DBAppException("primary key already in database");
			return res < 0;
		} else if (newEntry instanceof Date) {
			res = ((Date) newEntry).compareTo((Date) lastEntry);
			if(res == 0)
				throw new DBAppException("primary key already in database");
			return res < 0;
		} else 
			throw new DBAppException("Type not allowed");

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
			
			Calendar calMin = Calendar.getInstance();
			calMin.setTime(min2);
			calMin.set(Calendar.HOUR_OF_DAY, 0);
			calMin.set(Calendar.MINUTE, 0);
			calMin.set(Calendar.SECOND, 0);
			min2 = calMin.getTime();
			
			Calendar calMax = Calendar.getInstance();
			calMax.setTime(max2);
			calMax.set(Calendar.HOUR_OF_DAY, 0);
			calMax.set(Calendar.MINUTE, 0);
			calMax.set(Calendar.SECOND, 0);
			max2 = calMax.getTime();
			
			Calendar calVal = Calendar.getInstance();
			calVal.setTime(value2);
			calVal.set(Calendar.HOUR_OF_DAY, 0);
			calVal.set(Calendar.MINUTE, 0);
			calVal.set(Calendar.SECOND, 0);
			value2 = calVal.getTime();

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

	public static String dateToString(Date d)
	{
		String year;
		String month;
		String day;
		
		year = (d.getYear() + 1900) + "";
		
		if((d.getMonth() + 1) < 10)
			month = "0" + (d.getMonth()+1);
		else
			month = d.getMonth() + 1 +"";
		
		if(d.getDate() < 10)
			day = "0" + (d.getDate());
		else
			day = d.getDate() + "";
		
		return year + "-" + month + "-" + day;
	}
	

}