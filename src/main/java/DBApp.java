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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class DBApp implements DBAppInterface {

	static int rowCount;
	static String dataPath = "src/main/resources/data";

	@Override
	public void init() {

		File ParentFile = new File(dataPath);

		if (!ParentFile.exists()) {
			ParentFile.mkdir();
		}

		Properties prop = new Properties();
		try {
			String fileName = "DBApp.config";
//			ClassLoader classLoader = DBApp.class.getClassLoader();
//
//			URL res = Objects.requireNonNull(classLoader.getResource(fileName),
//					"Can't find configuration file DBApp.config");

			InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);

			prop.load(is);

			rowCount = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
			System.out.println(rowCount);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		File tableCheck = new File(dataPath + "/" + tableName);
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

	public void updatePageList(String tableName, Vector<Page> pageList) {
		try {
			FileOutputStream f1 = new FileOutputStream(dataPath + "/" + tableName + "/PageList.class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(pageList);
			out.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		if (!(new File(dataPath + "/" + tableName).exists()))
			throw new DBAppException("table doesnt exist");
		Table t = new Table(tableName);
		TableContent content = new TableContent(tableName);
		String primaryKey = "";
		try {
			if (colNameValue.size() != content.columns.size())
				throw new DBAppException("missing attribute or table data is already in metadata file");
			primaryKey = verifyColumnTypes(tableName, colNameValue, content);
			if (primaryKey.equals("-1"))
				throw new DBAppException("Wrong type");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Vector<Page> pageList = deserializePageList(tableName);

		if (pageList.isEmpty()) {
			t.addPage();
			Vector<Hashtable<String, Object>> vec = new Vector<Hashtable<String,Object>>(rowCount);
			vec.add(colNameValue);
			Page page = new Page(dataPath + "/" + tableName + "/Page" + 1 + ".class");
			page.min = colNameValue.get(primaryKey);
			page.max = colNameValue.get(primaryKey);
			pageList.add(page);
			serialize(page.path, vec);
			updatePageList(tableName, pageList);

		} else {

			int i = (decidePage(pageList, colNameValue, primaryKey));

			Vector<Hashtable<String, Object>> page = deserializePage(pageList.get(i).path);
			if (page.size() > rowCount - 1) {
				Page page2 = new Page(dataPath + "/" + tableName + "/Page" + (i + 2) + ".class");
				t.addPage();

				Vector<Vector<Hashtable<String, Object>>> temp = split(page, colNameValue, primaryKey);
				page2.min = temp.get(1).firstElement().get(primaryKey);
				page2.max = temp.get(1).lastElement().get(primaryKey);
				page2.size = temp.get(1).size();
				pageList.get(i).min = temp.get(0).firstElement().get(primaryKey);
				pageList.get(i).max = temp.get(0).lastElement().get(primaryKey);
				pageList.get(i).size = temp.get(0).size();
				pageList.add(i, page2);
				Page tempPage = pageList.get(i + 1);
				pageList.remove(tempPage);
				pageList.add(i, tempPage);
				serialize(page2.path, temp.get(1));
				serialize(pageList.get(i).path, temp.get(0));
				Collections.sort(pageList);
				t.updatePageList(pageList);

			} else {
				page.add(decideIndex(page, colNameValue,primaryKey),colNameValue);

				pageList.get(i).min = page.firstElement().get(primaryKey);
				pageList.get(i).max = page.lastElement().get(primaryKey);
				pageList.get(i).size = page.size();
				serialize(pageList.get(i).path, page);
				Collections.sort(pageList);
				t.updatePageList(pageList);
			}
		}

	}

	private int decidePage(Vector<Page> pageList, Hashtable<String, Object> colNameValue, String primaryKey) throws DBAppException {
		int i = 0;
		for (i = 0; i < pageList.size(); i++) 
			if(Compare(colNameValue.get(primaryKey), pageList.get(i).min))
				continue;
			else
				break;
		
		return i-1>-1? i-1:0;
	}

	public static Vector<Vector<Hashtable<String, Object>>> split(Vector<Hashtable<String, Object>> page,
			Hashtable<String, Object> record, String primaryKey) throws DBAppException {
		page.add(decideIndex(page,record, primaryKey),record);
		Vector<Vector<Hashtable<String, Object>>> res = new Vector<Vector<Hashtable<String, Object>>>(2);
		res.add(new Vector<Hashtable<String, Object>>(rowCount));
		res.add(new Vector<Hashtable<String, Object>>(rowCount));
		for (int i = 0; i < page.size(); i++) {
			if (i < page.size() / 2) {
				res.get(0).add(page.get(i));
			} else
				res.get(1).add(page.get(i));

		}
		return res;
	}

	public static int decideIndex(Vector<Hashtable<String, Object>> l,Hashtable<String, Object> target,Object primaryKey) throws DBAppException
	{
		int min=0;
		int max=l.size()-1;
		int mid=(min+max)/2;
		while(min<=max)
		{
			if(Compare(l.get(mid).get(primaryKey),target.get(primaryKey)))
				max=mid-1;
			else if(Compare(target.get(primaryKey),l.get(mid).get(primaryKey)))
				min=mid+1;
			else
				return mid;
			mid=(min+max)/2;
		}
		return min;
	}

	public String verifyColumnTypes(String tableName, Hashtable<String, Object> colNameValue, TableContent content)
			throws ClassNotFoundException, DBAppException, ParseException {


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
						if (!minmax(content.columns.get(i).min, content.columns.get(i).max, obj.toString(),content.columns.get(i).type)) 
							throw new DBAppException("The value is out of range");
						
						if ((Boolean.parseBoolean(content.columns.get(i).isCluster.toLowerCase())))
							primaryKeyColumn = key;

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

	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {

		if (clusteringKeyValue.charAt(0) == '0')
			clusteringKeyValue = clusteringKeyValue.substring(1);

		//Table tableReferance = new Table(tableName);
		TableContent TC = new TableContent(tableName);
		Vector<Page> pageList = deserializePageList(tableName);
		boolean pKeyNotFound = true;

		for (int pageIndex = 0; pageIndex < pageList.size(); pageIndex++) {
			Vector<Hashtable<String, Object>> vec = deserializePage(pageList.get(pageIndex).path);
			for (int i = 0; i < vec.size(); i++) {
				Hashtable<String, Object> arrHtbl = vec.get(i);
				boolean flag = false;

				Enumeration<String> keys1 = arrHtbl.keys();
				while (keys1.hasMoreElements()) {
					String value;
					String key = keys1.nextElement();
					if (arrHtbl.get(key) instanceof Date) {
						value = dateToString((Date) arrHtbl.get(key));
					}

					else
						value = arrHtbl.get(key).toString(); // WE CHANGED THIS FROM STRING TO OBJECT

					if (key.equals(TC.getPrimaryKey(tableName)) && (value.equals(clusteringKeyValue))) {
						flag = true;
						pKeyNotFound = false;
						break;
					}
				}

				//Enumeration<String> keys2 = arrHtbl.keys();

				if (flag) {
					Enumeration<String> keys = columnNameValue.keys();

					while (keys.hasMoreElements()) {
						String key = keys.nextElement();
						if (!arrHtbl.containsKey(key)) {
							throw new DBAppException("Invalid Column Name");
						}
						Object value = columnNameValue.get(key);
						arrHtbl.replace(key, value);
					}
//	        	         
					serialize(pageList.get(pageIndex).path, vec);
				}

			}
		}

		if (pKeyNotFound)
			throw new DBAppException("Record not found");

	}
	
	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		Table t = new Table(tableName);
		Vector<Hashtable<String, Object>> page;
		Vector<Page> pageList = deserializePageList(tableName);
		TableContent content = new TableContent(tableName);
		Set<Entry<String, Object>> record = columnNameValue.entrySet();

		@SuppressWarnings("unused")
		boolean removed = false;
		if (!(new File(dataPath + "/" + tableName).exists()))
			throw new DBAppException("table doesnt exist");
		String primaryKey = "";
		try {
			primaryKey = verifyColumnTypes(tableName, columnNameValue, content);

		} catch (ClassNotFoundException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (primaryKey.equals("-1"))
			throw new DBAppException("Type mismatch");

		if (pageList.isEmpty()) { 
			throw new DBAppException("Table is empty");
		} else {
			for (int i = 0; i < pageList.size(); i++) { 

				if (i >= pageList.size()) {
					throw new DBAppException("record not in table");
				}

				page = deserializePage(pageList.get(i).path);

				for (int j = 0; j < page.size(); j++) {
					Set<Entry<String, Object>> row = page.get(j).entrySet();
					if (row.containsAll(record)) {
						removed = true;
						page.remove(j);
						if (page.isEmpty()) {
							t.deletePage(pageList.get(i).path);
							pageList.remove(i);
							t.updatePageList(pageList);
						} else
							serialize(pageList.get(i).path, page);
					}
				}
			}
		}

	}

	@SuppressWarnings("rawtypes")
	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		ArrayList res = new ArrayList();
		SQLTerm firstOperant = sqlTerms[0];
		SQLTerm secondOperant = sqlTerms[1];
		Vector<Page> pageListOfFirstOperant = deserializePageList(firstOperant.strTableName);
		Vector<Page> pageListOfsecondOperant = deserializePageList(secondOperant.strTableName);
		String typeOfFirstOperant = firstOperant.getObjValue().getClass().getSimpleName();
		for (int i = 0; i < pageListOfFirstOperant.size(); i++) {
			Vector<Hashtable<String, Object>> records = deserializePage(((Page)pageListOfFirstOperant.get(i)).path);
			for(Hashtable<String, Object>h :records) {
				Enumeration<String> enumeration = h.keys();
				while(enumeration.hasMoreElements()) {
					String key = enumeration.nextElement();
					if(key.equals(firstOperant.strColumnName)) {
						switch (typeOfFirstOperant) {
						case "Double":
							Double value1 = Double.valueOf((String) firstOperant.getObjValue());
							Double value2 = Double.valueOf((String) h.get(key));
							if(value1==value2) {
								//res.add(h.)
							}
							break;

						default:
							break;
						}
					}
				}
			}
		}
		return null;
	}

	public static boolean checkType(Object obj, String c) throws ClassNotFoundException {
		return Class.forName(c).isInstance(obj);
	}

	


	@SuppressWarnings("unchecked")
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
			FileInputStream fileIn = new FileInputStream(dataPath + "/" + tableName + "/PageList.class");
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
			if (res == 0)
				throw new DBAppException("primary key already in database");
			return res < 0;
		} else
			throw new DBAppException("Type not allowed");
	}

	@SuppressWarnings("deprecation")
	public static boolean minmax(String min, String max, String value, String type)
			throws ClassNotFoundException, ParseException {
		boolean flag = false;
		@SuppressWarnings("rawtypes")
		Class x = Class.forName(type);
		if (x.getSimpleName().equals("Integer")) {
			Integer min2 = Integer.valueOf(min);
			Integer max2 = Integer.valueOf(max);
			Integer value2 = Integer.valueOf(value);
			if (min2 <= value2 && value2 <= max2) {
				flag = true;
			}
		}
		if (x.getSimpleName().equals("Date")) {
			String[] minArr = min.split("-");
			int[] minInt = {Integer.parseInt(minArr[0])-1900,Integer.parseInt(minArr[1])-1,Integer.parseInt(minArr[2])};
			Date min2 = new Date(minInt[0],minInt[1],minInt[2]);
			String[] maxArr = max.split("-");
			int[] maxInt = {Integer.parseInt(maxArr[0]),Integer.parseInt(maxArr[1]),Integer.parseInt(maxArr[2])};
			Date max2 = new Date(maxInt[0]-1900,maxInt[1]-1,maxInt[2]);
			Date value2 = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy").parse(value);
			
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
			if ((value2.after(min2) && value2.before(max2)) || value2.compareTo(max2)==0 || value2.compareTo(min2) == 0 ) 
				flag = true;
			
		}
		if (x.getSimpleName().equals("Double")) {
			Double min2 = Double.valueOf(min);
			Double max2 = Double.valueOf(max);
			Double value2 = Double.valueOf(value);
			if (min2 <= value2 && value2 <= max2) {
				flag = true;
			}
		}
		if (x.getSimpleName().equals("String")) {
			if ((min.length() <= value.length()) && (max.length() >= value.length())) {
				flag = true;
			} else if ((min.compareToIgnoreCase(value) <= 0) && (max.compareToIgnoreCase(value) >= 0))
				flag = true;
		}
		return flag;
	}

	@SuppressWarnings("deprecation")
	public static String dateToString(Date d) {
		String year;
		String month;
		String day;

		year = (d.getYear() + 1900) + "";

		if ((d.getMonth() + 1) < 10)
			month = "0" + (d.getMonth() + 1);
		else
			month = d.getMonth() + 1 + "";

		if (d.getDate() < 10)
			day = "0" + (d.getDate());
		else
			day = d.getDate() + "";

		return year + "-" + month + "-" + day;
	}

}