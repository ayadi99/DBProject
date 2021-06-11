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
	static int bucketSize = 100;
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
		TableContent tc = new TableContent(tableName);
		GridIndex grid = new GridIndex(tableName, columnNames, tc);

		File gridFolder = new File(dataPath + "/" + tableName + "/GridFolder");
		if (!gridFolder.exists()) {
			gridFolder.mkdir();
		}

		File f = new File(dataPath + "/" + tableName + "/GridFolder" + "/GridList.class");
		Vector<Hashtable<String, String[]>> gridMetaData;

		if (f.exists())
			gridMetaData = deserializeGridList(tableName);
		else
			gridMetaData = new Vector<Hashtable<String, String[]>>();

		for (Hashtable<String, String[]> hashtable : gridMetaData) {
			String[] arr = hashtable.get(hashtable.keys().nextElement());
			Arrays.sort(arr);
			Arrays.sort(columnNames);
			if (Arrays.equals(arr, columnNames))
				throw new DBAppException("Grid already exists");
		}

		String gridID = "";

		String primaryKey = tc.getPrimaryKey(tableName);

		int indexDimensions = columnNames.length;

		for (String str : columnNames) {
			if (str.equals(primaryKey))
				gridID += "p";
		}

		int gridsCount = new File(dataPath + "/" + tableName + "/GridFolder").listFiles().length;

		if (gridsCount != 0)
			gridsCount--;

		gridID += (indexDimensions + "d" + gridsCount);

		Hashtable<String, String[]> newIndex = new Hashtable<String, String[]>();
		newIndex.put(gridID, columnNames);
		gridMetaData.add(newIndex);

		serializeGridList(dataPath + "/" + tableName + "/GridFolder" + "/GridList.class", gridMetaData);
		grid = fillGrid(tableName, grid, primaryKey);
		serializeGrid(tableName, grid, gridsCount);

	}

	public GridIndex fillGrid(String tableName, GridIndex grid, String primaryKey) {
		// TODO Auto-generated method stub
		// int configSize = 100;//get from config
		Vector<Page> pageList = deserializePageList(tableName);
		for (int i = 0; i < pageList.size(); i++) {
			Vector<Hashtable<String, Object>> page = deserializePage(pageList.get(i).path);
			Iterator<Hashtable<String, Object>> pageIterator = page.iterator();
			while (pageIterator.hasNext()) {
				Hashtable<String, Object> record = pageIterator.next();
				int[] coord = grid.getRecordCoordinates(record);
				Vector<Bucket> bucketList = grid.get(coord);
				boolean added = false;
				for (int j = 0; j < bucketList.size(); j++) {
					if (bucketList.get(j).values.size() >= bucketSize)// convert to size from config
						continue;
					else {
						bucketList.get(j).values
								.add(new records(pageList.get(i).path, page.indexOf(record), record.get(primaryKey)));
						added = true;
						break;
					}
				}
				if (!added) {
					Bucket b = new Bucket(bucketSize);
					b.values.add(new records(pageList.get(i).path, page.indexOf(record), record.get(primaryKey)));
					bucketList.add(b);
				}

				grid.set(coord, bucketList);
			}
		}

		return grid;
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
		Vector<Hashtable<String, String[]>> gridList;
		if ((new File(dataPath + "/" + tableName + "/GridFolder").exists()))
			gridList = deserializeGridList(tableName);
		else
			gridList = new Vector<Hashtable<String, String[]>>();

		if (pageList.isEmpty()) {
			t.addPage();
			Vector<Hashtable<String, Object>> vec = new Vector<Hashtable<String, Object>>(rowCount);
			vec.add(colNameValue);
			Page page = new Page(dataPath + "/" + tableName + "/Page" + 1 + ".class");
			page.min = colNameValue.get(primaryKey);
			page.max = colNameValue.get(primaryKey);
			pageList.add(page);
			for (Hashtable<String, String[]> gridData : gridList) {
				String key = gridData.keySet().iterator().next();
				if (key.charAt(0) == 'p')
					key = key.substring(3);
				else
					key.substring(2);
				GridIndex grid = deserializeGrid(tableName, Integer.parseInt(key));
				updatPageRecords(vec, grid, primaryKey, page.path);
				serializeGrid(tableName, grid, Integer.parseInt(key));
			}
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

				for (Hashtable<String, String[]> gridData : gridList) {
					String key = gridData.keySet().iterator().next();
					if (key.charAt(0) == 'p')
						key = key.substring(3);
					else
						key.substring(2);
					GridIndex grid = deserializeGrid(tableName, Integer.parseInt(key));
					updatPageRecords(temp.get(0), grid, primaryKey, pageList.get(i).path);
					updatPageRecords(temp.get(1), grid, primaryKey, page2.path);
					serializeGrid(tableName, grid, Integer.parseInt(key));
				}

				serialize(page2.path, temp.get(1));
				serialize(pageList.get(i).path, temp.get(0));
				Collections.sort(pageList);
				t.updatePageList(pageList);

			} else {
				page.add(decideIndex(page, colNameValue, primaryKey), colNameValue);

				pageList.get(i).min = page.firstElement().get(primaryKey);
				pageList.get(i).max = page.lastElement().get(primaryKey);
				pageList.get(i).size = page.size();

				for (Hashtable<String, String[]> gridData : gridList) {
					String key = gridData.keySet().iterator().next();
					if (key.charAt(0) == 'p')
						key = key.substring(3);
					else
						key.substring(2);
					GridIndex grid = deserializeGrid(tableName, Integer.parseInt(key));
					updatPageRecords(page, grid, primaryKey, pageList.get(i).path);
					serializeGrid(tableName, grid, Integer.parseInt(key));
				}

				serialize(pageList.get(i).path, page);
				Collections.sort(pageList);
				t.updatePageList(pageList);
			}
		}

	}

	private int decidePage(Vector<Page> pageList, Hashtable<String, Object> colNameValue, String primaryKey)
			throws DBAppException {
		int i = 0;
		for (i = 0; i < pageList.size(); i++)
			if (Compare(colNameValue.get(primaryKey), pageList.get(i).min))
				continue;
			else
				break;

		return i - 1 > -1 ? i - 1 : 0;
	}

	public static Vector<Vector<Hashtable<String, Object>>> split(Vector<Hashtable<String, Object>> page,
			Hashtable<String, Object> record, String primaryKey) throws DBAppException {
		page.add(decideIndex(page, record, primaryKey), record);
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

	public static int decideIndex(Vector<Hashtable<String, Object>> l, Hashtable<String, Object> target,
			Object primaryKey) throws DBAppException {
		int min = 0;
		int max = l.size() - 1;
		int mid = (min + max) / 2;
		while (min <= max) {
			if (Compare(l.get(mid).get(primaryKey), target.get(primaryKey)))
				max = mid - 1;
			else if (Compare(target.get(primaryKey), l.get(mid).get(primaryKey)))
				min = mid + 1;
			else
				return mid;
			mid = (min + max) / 2;
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
						if (!minmax(content.columns.get(i).min, content.columns.get(i).max, obj.toString(),
								content.columns.get(i).type))
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

	@SuppressWarnings("unused")
	private void updatPageRecords(Vector<Hashtable<String, Object>> page, GridIndex grid, String primaryKey,
			String pagePath) throws DBAppException {
		// TODO Auto-generated method stub
		for (int i = 0; i < page.size(); i++) {
			int[] coord = grid.getRecordCoordinates(page.get(i));
			Vector<Bucket> bucketList = grid.get(coord);
			Bucket target = null;
			for (int j = 0; j < bucketList.size(); j++)
				if (bucketList.get(j).values.contains(new records(page.get(i).get(primaryKey))))
					target = bucketList.get(j);
			if (target == null) {
				records rec = new records(pagePath, i, page.get(i).get(primaryKey));
				if (bucketList.get(bucketList.size() - 1).values.size() >= bucketSize)
					bucketList.add(new Bucket(bucketSize));
				bucketList.get(bucketList.size() - 1).values.add(rec);
			} else {
				records rec = target.values.get(target.values.indexOf(new records(page.get(i).get(primaryKey))));
				rec.indexInPage = i;
				rec.path = pagePath;
			}
		}
	}

	public int getIndexNumber(String str) {
		String output = "";

		int index;
		for (index = 0; str.charAt(index) != 'd'; index++)
			;

		output = str.substring(index + 1);
		return Integer.parseInt(output);

	}

	public int getIndexDimensions(String str) {
		String output = "";

		if (str.charAt(0) == 'p')
			str = str.substring(1);

		for (int i = 0; str.charAt(i) != 'd'; i++) {
			output += str.charAt(i);
		}

		return Integer.parseInt(output);

	}

	public boolean arraysAreEqual(String[] strArr1, ArrayList<String> strArr2) {
		for (String s1 : strArr1) {
			boolean s1IsInS2 = false;
			for (String s2 : strArr2) {
				if (s2.equals(s1)) {
					s1IsInS2 = true;
					break;
				}

			}
			if (!s1IsInS2)
				return false;
		}
		return true;

	}

	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		

		Boolean useIndex = false;
		int indexNum = -1;

		File gridListFile = new File(dataPath + "/" + tableName + "/GridFolder/GridList.class");
		if (!gridListFile.exists()) {
			updateTableWithoutIndex(tableName, clusteringKeyValue, columnNameValue);
			System.out.println("No indices");
			return;
		}

		else {
			Vector<Hashtable<String, String[]>> gridIndices = deserializeGridList(tableName);
			loopVector: for (int i = 0; i < gridIndices.size(); i++) {
				Hashtable<String, String[]> ht = gridIndices.get(i);
				Enumeration<String> e = ht.keys();
				while (e.hasMoreElements()) {
					String key = e.nextElement();
					if (key.charAt(0) == 'p') {
						// System.out.println("primary");
						if (getIndexDimensions(key) == 1) {
							// System.out.println("1 dimension");
							String[] strArr = ht.get(key);
							TableContent tc = new TableContent(tableName);
							if (strArr[0].equals(tc.getPrimaryKey(tableName))) {
								useIndex = true;
								indexNum = getIndexNumber(key);
							}
						} else {
							continue loopVector;
						}
					} else
						continue loopVector;
				}
			}

		}

		if (!useIndex) {
			System.out.println("withut index");
			updateTableWithoutIndex(tableName, clusteringKeyValue, columnNameValue);
			return;
		} else {
			System.out.println("With index");
			GridIndex grid = deserializeGrid(tableName, indexNum);

			Hashtable<String, Object> record = new Hashtable<String, Object>();
			TableContent tc = new TableContent(tableName);
			String primaryKey = tc.getPrimaryKey(tableName);
			record.put(primaryKey, clusteringKeyValue);
			int[] recordCoordinates = grid.getRecordCoordinates(record);

			Vector<Bucket> vecBuckets = grid.get(recordCoordinates);

			parentLoop: for (Bucket buck : vecBuckets) {
				Vector<records> recs = buck.getRecord();

				for (records rec : recs) {
					if ((rec.primaryKey).equals(clusteringKeyValue)) {

						Vector<Hashtable<String, Object>> page = deserializePage(rec.path);

						Hashtable<String, Object> recordInTable = page.get(rec.indexInPage);
						
						
						
						
						Enumeration<String> keys = columnNameValue.keys();

						while (keys.hasMoreElements()) {
							String key = keys.nextElement();
							if (!recordInTable.containsKey(key)) {
								throw new DBAppException("Invalid Column Name");
							}
							Object value = columnNameValue.get(key);
							recordInTable.replace(key, value);
						}

						page.add(rec.indexInPage, recordInTable);
						serialize(rec.path, page);
						updatPageRecords(page, grid, primaryKey, rec.path);
						

						return;
					}

				}
			}

		}

	}

	public void updateTableWithoutIndex(String tableName, String clusteringKeyValue,
			Hashtable<String, Object> columnNameValue) throws DBAppException {

		if (clusteringKeyValue.charAt(0) == '0')
			clusteringKeyValue = clusteringKeyValue.substring(1);

		// Table tableReferance = new Table(tableName);
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

				// Enumeration<String> keys2 = arrHtbl.keys();

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
		
		File f = new File(dataPath + "/" + tableName + "/GridFolder/GridList.class");
		
		boolean indexFound = false ;
		int gridIndex = -1 ;
		
		if(f.exists()) {
			 Enumeration<String> keys = columnNameValue.keys();
			 ArrayList<String> originalKeys = new ArrayList<String>() ;
			 while(keys.hasMoreElements()) {
				 String key = keys.nextElement();
				 originalKeys.add(key);
			 }

			 
		        Vector<Hashtable<String,String[]>> gridIndices = this.deserializeGridList(tableName);
		       
			        for(int i = 0 ; i < gridIndices.size() ; i++ ) {
			        Hashtable<String,String[]> ht = gridIndices.get(i);
			        Enumeration<String> e = ht.keys();
				        while(e.hasMoreElements()) {
				        	String key = e.nextElement();
				        	int dimension = getIndexDimensions(key) ;
				        	if(dimension == originalKeys.size()) {
				        		String[] columns = ht.get(key);
				        		if(arraysAreEqual(columns,originalKeys)) {
				        			indexFound = true ;
				        		    gridIndex = getIndexNumber(key) ; 
				        		}
				        		
				        		
				        	}

				        }
			        }
			        
			        if(indexFound) {
			        	
			            	GridIndex grid = deserializeGrid(tableName, gridIndex);
			            	int[] coordinates = grid.getRecordCoordinates(columnNameValue);

			            	

			            	Vector<Bucket> vecBuckets = grid.get(coordinates);
			    			
			    			parentLoop: for(Bucket buck : vecBuckets) {
			    				Vector<records> recs = buck.getRecord();
			    				
			    				for(records rec : recs) {
			    					System.out.println("delete with index");
			    					Vector<Hashtable<String, Object>> page = deserializePage(rec.path);
		    						Hashtable<String,Object> recordInTable = page.get(rec.indexInPage);
		    						
			    					 Set<Entry<String, Object>> e1 = columnNameValue.entrySet();
			    					 Set<Entry<String, Object>> e2 = recordInTable.entrySet();

			    					if(e2.containsAll(e1)) {
			    						System.out.println("hashtables w keda");
			    						TableContent tc = new TableContent(tableName);
			    						String primaryKey = tc.getPrimaryKey(tableName);
			    						
			    						
			    						page.remove(page.indexOf(recordInTable));
			    						
			    						updatPageRecords(page, grid, primaryKey, rec.path);
			    						
			    						serialize(rec.path, page);
			    					}
			    				}
			    			}
			        }
			        else {
			        	deleteFromTableWithoutIndex(tableName, columnNameValue);
			        	return;
			        }
		     
		     }
				else {
					deleteFromTableWithoutIndex(tableName, columnNameValue);
					return;
				}

		}

	public void deleteFromTableWithoutIndex(String tableName, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		
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
	
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException{
		Vector<Hashtable<String, Object>> result_set = new Vector<Hashtable<String, Object>>();
		File f = new File(dataPath+"/"+sqlTerms[0]._strTableName+"/GridFolder/GridList.class");
		String operator = arrayOperators[0];
		if(!f.exists()) {
			return selectFromTable2(sqlTerms, arrayOperators);
		}
		Vector<Hashtable<String,String[]>> gridIndices = deserializeGridList(sqlTerms[0]._strTableName);
		boolean flag = false;
		String index = "";
		String [] col_name = new String[sqlTerms.length];
		for (int i = 0; i < col_name.length; i++) {
			col_name[i] = sqlTerms[i]._strColumnName;
			
		}
		Arrays.sort(col_name);
		for(Hashtable<String, String[]> h:gridIndices) {
			Enumeration<String> enmuration = h.keys();
			while(enmuration.hasMoreElements()) {
				String key = enmuration.nextElement();
				Arrays.sort(h.get(key));
				if(Arrays.equals(h.get(key), col_name)) {
					flag = true;
					for(int i=0;i<key.length();i++) {
						if(key.charAt(i)=='d') {
							index = key.substring(i+1);
						}
					}
				}
				
				}
				
			}
		
		if(flag) {
			
			int ind = Integer.parseInt(index);
			GridIndex grid = deserializeGrid(sqlTerms[0]._strTableName, ind);
			Hashtable<String, Object> acess = new Hashtable<String,Object>();
			for(SQLTerm s:sqlTerms) {
				acess.put(s._strColumnName,s._objValue);
			}
			int [] cord = grid.getRecordCoordinates(acess);
			Vector<Bucket> Buck = grid.get(cord);
			TableColumns[] content = grid.getGridContent();
			if(operator=="OR") {
				for(SQLTerm s :sqlTerms) {
					if(s._strOperator=="=") {
						for(Bucket b:Buck) {
							for(records r:b.getRecord()) {
								Vector<Hashtable<String, Object>> p=deserializePage(r.path);
								Hashtable<String, Object> value = p.get(r.indexInPage);
								if(value.containsValue(s._objValue)&&value.containsKey(s._strColumnName)) {
									if(!result_set.contains(value)) {
										result_set.add(value);
									}
								}
							}
						}
					}
					else if(s._strOperator=="<") {
						
						for(int i=0;i<content.length;i++) {
							if(content[i].name==s._strColumnName) {
								for(int j=0;j<i;j++) {
									cord[i]=j;
									Vector<Bucket> Buck2 = grid.get(cord);
									for(Bucket b:Buck2) {
										for(records r:b.getRecord()) {
											Vector<Hashtable<String, Object>> p=deserializePage(r.path);
											Hashtable<String, Object> value = p.get(r.indexInPage);
											Enumeration<String> enumuration = value.keys();
											while(enumuration.hasMoreElements()) {
												String key = enumuration.nextElement();
												if(key.equals(s._strColumnName)) {
													if(Compare2(s._objValue,value.get(key),false)) {
														if(!result_set.contains(value)) {
															result_set.add(value);
														}
													}
												}
											}
											
										}
									}
								}
							}
						}
					}
					else if(s._strOperator=="<=") {
						
						for(int i=0;i<content.length;i++) {
							if(content[i].name==s._strColumnName) {
								for(int j=0;j<=i;j++) {
									cord[i]=j;
									Vector<Bucket> Buck2 = grid.get(cord);
									for(Bucket b:Buck2) {
										for(records r:b.getRecord()) {
											Vector<Hashtable<String, Object>> p=deserializePage(r.path);
											Hashtable<String, Object> value = p.get(r.indexInPage);
											Enumeration<String> enumuration = value.keys();
											while(enumuration.hasMoreElements()) {
												String key = enumuration.nextElement();
												if(key.equals(s._strColumnName)) {
													if(Compare2(s._objValue,value.get(key),true)) {
														if(!result_set.contains(value)) {
															result_set.add(value);
														}
													}
												}
											}
											
										}
									}
								}
							}
						}
					}
					else if(s._strOperator==">") {
						
						for(int i=0;i<content.length;i++) {
							if(content[i].name==s._strColumnName) {
								for(int j=9;j>=i;j--) {
									cord[i]=j;
									Vector<Bucket> Buck2 = grid.get(cord);
									for(Bucket b:Buck2) {
										for(records r:b.getRecord()) {
											Vector<Hashtable<String, Object>> p=deserializePage(r.path);
											Hashtable<String, Object> value = p.get(r.indexInPage);
											Enumeration<String> enumuration = value.keys();
											while(enumuration.hasMoreElements()) {
												String key = enumuration.nextElement();
												if(key.equals(s._strColumnName)) {
													if(Compare2(value.get(key),s._objValue,false)) {
														if(!result_set.contains(value)) {
															result_set.add(value);
														}
													}
												}
											}
											
										}
									}
								}
							}
						}
					}
					else if(s._strOperator==">=") {
						
						for(int i=0;i<content.length;i++) {
							if(content[i].name==s._strColumnName) {
								for(int j=9;j>=i;j--) {
									cord[i]=j;
									Vector<Bucket> Buck2 = grid.get(cord);
									for(Bucket b:Buck2) {
										for(records r:b.getRecord()) {
											Vector<Hashtable<String, Object>> p=deserializePage(r.path);
											Hashtable<String, Object> value = p.get(r.indexInPage);
											Enumeration<String> enumuration = value.keys();
											while(enumuration.hasMoreElements()) {
												String key = enumuration.nextElement();
												if(key.equals(s._strColumnName)) {
													if(Compare2(value.get(key),s._objValue,true)) {
														if(!result_set.contains(value)) {
															result_set.add(value);
														}
													}
												}
											}
											
										}
									}
								}
							}
						}
					}
					else if(s._strOperator=="!=") {
						for(Bucket b:Buck) {
							for(records r:b.getRecord()) {
								Vector<Hashtable<String, Object>> p=deserializePage(r.path);
								Hashtable<String, Object> value = p.get(r.indexInPage);
								if(!(value.containsValue(s._objValue)&&value.containsKey(s._strColumnName))) {
									if(!result_set.contains(value)) {
										result_set.add(value);
									}
								}
							}
						}
					}
				}
			}
			else if(operator=="AND") {
				ArrayList<Hashtable<String, Object>> temp = new ArrayList<Hashtable<String,Object>>();
				for(Bucket b:Buck) {
					for(records r:b.getRecord()) {
						int counter =0;

						for(SQLTerm s:sqlTerms) {

							if(s._strOperator=="=") {
								Vector<Hashtable<String, Object>> p=deserializePage(r.path);
								Hashtable<String, Object> value = p.get(r.indexInPage);
								if(value.containsValue(s._objValue)&&value.containsKey(s._strColumnName)) {
									
									counter++;
									temp.add(value);
								}
							}
							else if(s._strOperator==">=") {
								for(int i=0;i<content.length;i++) {
									if(content[i].name==s._strColumnName) {
										for(int j=9;j>=i;j--) {
											cord[i]=j;
											Vector<Bucket> Buck2 = grid.get(cord);
											for(Bucket b2:Buck2) {
												for(records r2:b2.getRecord()) {
													Vector<Hashtable<String, Object>> p=deserializePage(r2.path);
													Hashtable<String, Object> value = p.get(r2.indexInPage);
													Enumeration<String> enumuration = value.keys();
													while(enumuration.hasMoreElements()) {
														String key = enumuration.nextElement();
														if(key.equals(s._strColumnName)) {
															if(Compare2(value.get(key),s._objValue,true)) {
																counter++;
																temp.add(value);
															}
														}
													}
													
												}
											}
										}
									}
								}
							}
							else if(s._strOperator=="<") {
								for(int i=0;i<content.length;i++) {
									if(content[i].name==s._strColumnName) {
										for(int j=0;j<i;j++) {
											cord[i]=j;
											Vector<Bucket> Buck2 = grid.get(cord);
											for(Bucket b2:Buck2) {
												for(records r2:b2.getRecord()) {
													Vector<Hashtable<String, Object>> p=deserializePage(r2.path);
													Hashtable<String, Object> value = p.get(r2.indexInPage);
													Enumeration<String> enumuration = value.keys();
													while(enumuration.hasMoreElements()) {
														String key = enumuration.nextElement();
														if(key.equals(s._strColumnName)) {
															if(Compare2(s._objValue,value.get(key),false)) {
																counter++;
																temp.add(value);
															}
														}
													}
													
												}
											}
										}
									}
								}
							}
							else if(s._strOperator=="<=") {
								for(int i=0;i<content.length;i++) {
									if(content[i].name==s._strColumnName) {
										for(int j=0;j<=i;j++) {
											cord[i]=j;
											Vector<Bucket> Buck2 = grid.get(cord);
											for(Bucket b2:Buck2) {
												for(records r2:b2.getRecord()) {
													Vector<Hashtable<String, Object>> p=deserializePage(r2.path);
													Hashtable<String, Object> value = p.get(r2.indexInPage);
													Enumeration<String> enumuration = value.keys();
													while(enumuration.hasMoreElements()) {
														String key = enumuration.nextElement();
														if(key.equals(s._strColumnName)) {
															if(Compare2(s._objValue,value.get(key),true)) {
																counter++;
																temp.add(value);
															}
														}
													}
													
												}
											}
										}
									}
								}
							}
							else if(s._strOperator==">") {
								for(int i=0;i<content.length;i++) {
									if(content[i].name==s._strColumnName) {
										for(int j=9;j>i;j--) {
											cord[i]=j;
											Vector<Bucket> Buck2 = grid.get(cord);
											for(Bucket b2:Buck2) {
												for(records r2:b2.getRecord()) {
													Vector<Hashtable<String, Object>> p=deserializePage(r2.path);
													Hashtable<String, Object> value = p.get(r2.indexInPage);
													Enumeration<String> enumuration = value.keys();
													while(enumuration.hasMoreElements()) {
														String key = enumuration.nextElement();
														if(key.equals(s._strColumnName)) {
															if(Compare2(value.get(key),s._objValue,false)) {
																counter++;
																temp.add(value);
															}
														}
													}
													
												}
											}
										}
									}
								}
							}
							else if(s._strOperator=="!=") {
								Vector<Hashtable<String, Object>> p=deserializePage(r.path);
								Hashtable<String, Object> value = p.get(r.indexInPage);
								if(!(value.containsValue(s._objValue)&&value.containsKey(s._strColumnName))) {
									
									counter++;
									temp.add(value);
								}
							}
							
						}
						if(counter==sqlTerms.length) {
							for (int i = 0; i < temp.size(); i++) {
								if(!result_set.contains(temp.get(i))) {
									result_set.add(temp.get(i));
								}
							}
						}
					}
				}
			}
			else if(operator=="XOR") {
				ArrayList<Hashtable<String, Object>> temp = new ArrayList<Hashtable<String,Object>>();
				for(Bucket b:Buck) {
					for(records r:b.getRecord()) {
						int counter =0;

						for(SQLTerm s:sqlTerms) {

							if(s._strOperator=="=") {
								Vector<Hashtable<String, Object>> p=deserializePage(r.path);
								Hashtable<String, Object> value = p.get(r.indexInPage);
								if(value.containsValue(s._objValue)&&value.containsKey(s._strColumnName)) {
									
									counter++;
									temp.add(value);
								}
							}
							else if(s._strOperator==">=") {
								for(int i=0;i<content.length;i++) {
									if(content[i].name==s._strColumnName) {
										for(int j=9;j>=i;j--) {
											cord[i]=j;
											Vector<Bucket> Buck2 = grid.get(cord);
											for(Bucket b2:Buck2) {
												for(records r2:b2.getRecord()) {
													Vector<Hashtable<String, Object>> p=deserializePage(r2.path);
													Hashtable<String, Object> value = p.get(r2.indexInPage);
													Enumeration<String> enumuration = value.keys();
													while(enumuration.hasMoreElements()) {
														String key = enumuration.nextElement();
														if(key.equals(s._strColumnName)) {
															if(Compare2(value.get(key),s._objValue,true)) {
																counter++;
																temp.add(value);
															}
														}
													}
													
												}
											}
										}
									}
								}
							}
							else if(s._strOperator=="<") {
								for(int i=0;i<content.length;i++) {
									if(content[i].name==s._strColumnName) {
										for(int j=0;j<i;j++) {
											cord[i]=j;
											Vector<Bucket> Buck2 = grid.get(cord);
											for(Bucket b2:Buck2) {
												for(records r2:b2.getRecord()) {
													Vector<Hashtable<String, Object>> p=deserializePage(r2.path);
													Hashtable<String, Object> value = p.get(r2.indexInPage);
													Enumeration<String> enumuration = value.keys();
													while(enumuration.hasMoreElements()) {
														String key = enumuration.nextElement();
														if(key.equals(s._strColumnName)) {
															if(Compare2(s._objValue,value.get(key),false)) {
																counter++;
																temp.add(value);
															}
														}
													}
													
												}
											}
										}
									}
								}
							}
							else if(s._strOperator=="<=") {
								for(int i=0;i<content.length;i++) {
									if(content[i].name==s._strColumnName) {
										for(int j=0;j<=i;j++) {
											cord[i]=j;
											Vector<Bucket> Buck2 = grid.get(cord);
											for(Bucket b2:Buck2) {
												for(records r2:b2.getRecord()) {
													Vector<Hashtable<String, Object>> p=deserializePage(r2.path);
													Hashtable<String, Object> value = p.get(r2.indexInPage);
													Enumeration<String> enumuration = value.keys();
													while(enumuration.hasMoreElements()) {
														String key = enumuration.nextElement();
														if(key.equals(s._strColumnName)) {
															if(Compare2(s._objValue,value.get(key),true)) {
																counter++;
																temp.add(value);
															}
														}
													}
													
												}
											}
										}
									}
								}
							}
							else if(s._strOperator==">") {
								for(int i=0;i<content.length;i++) {
									if(content[i].name==s._strColumnName) {
										for(int j=9;j>i;j--) {
											cord[i]=j;
											Vector<Bucket> Buck2 = grid.get(cord);
											for(Bucket b2:Buck2) {
												for(records r2:b2.getRecord()) {
													Vector<Hashtable<String, Object>> p=deserializePage(r2.path);
													Hashtable<String, Object> value = p.get(r2.indexInPage);
													Enumeration<String> enumuration = value.keys();
													while(enumuration.hasMoreElements()) {
														String key = enumuration.nextElement();
														if(key.equals(s._strColumnName)) {
															if(Compare2(value.get(key),s._objValue,false)) {
																counter++;
																temp.add(value);
															}
														}
													}
													
												}
											}
										}
									}
								}
							}
							else if(s._strOperator=="!=") {
								Vector<Hashtable<String, Object>> p=deserializePage(r.path);
								Hashtable<String, Object> value = p.get(r.indexInPage);
								if(!(value.containsValue(s._objValue)&&value.containsKey(s._strColumnName))) {
									
									counter++;
									temp.add(value);
								}
							}
							
						}
						if(counter<sqlTerms.length&&counter!=0) {
							for (int i = 0; i < temp.size(); i++) {
								if(!result_set.contains(temp.get(i))) {
									result_set.add(temp.get(i));
								}
							}
						}
					}
				}
			}
			
		}
		else {
			return selectFromTable2(sqlTerms, arrayOperators);
		}
		return result_set.iterator();
	}
	
public Iterator selectFromTable2(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		Vector<Hashtable<String, Object>> result_set = new Vector<Hashtable<String, Object>>();
		String Operator = arrayOperators[0];
		SQLTerm first = sqlTerms[0];
		Vector<Page> pageListOfFirstOperant = deserializePageList(first._strTableName);
		if (Operator == "OR") {
			for (Page p : pageListOfFirstOperant) {
				Vector<Hashtable<String, Object>> records = deserializePage(p.path);
				for (SQLTerm sql : sqlTerms) {
					if (sql._strOperator == "=") {
						for (Hashtable<String, Object> h : records) {
							Enumeration<String> enumeration = h.keys();
							while (enumeration.hasMoreElements()) {
								String key = enumeration.nextElement();
								if (key.equals(sql._strColumnName)) {
									if (h.get(key).equals(sql._objValue)) {
										if (!result_set.contains(h)) {
											result_set.add(h);
										}
									}
								}
							}
						}
					} else if (sql._strOperator == ">") {
						for (Hashtable<String, Object> h : records) {
							Enumeration<String> enumeration = h.keys();
							while (enumeration.hasMoreElements()) {
								String key = enumeration.nextElement();
								if (key.equals(sql._strColumnName)) {
									if (Compare2(h.get(key), (sql._objValue), false)) {
										if (!result_set.contains(h)) {
											result_set.add(h);
										}
									}
								}
							}
						}
					} else if (sql._strOperator == ">=") {
						for (Hashtable<String, Object> h : records) {
							Enumeration<String> enumeration = h.keys();
							while (enumeration.hasMoreElements()) {
								String key = enumeration.nextElement();
								if (key.equals(sql._strColumnName)) {
									if (Compare2(h.get(key), (sql._objValue), true)) {
										if (!result_set.contains(h)) {
											result_set.add(h);
										}
									}
								}
							}
						}

					} else if (sql._strOperator == "<") {
						for (Hashtable<String, Object> h : records) {
							Enumeration<String> enumeration = h.keys();
							while (enumeration.hasMoreElements()) {
								String key = enumeration.nextElement();
								if (key.equals(sql._strColumnName)) {
									if (Compare2((sql._objValue), h.get(key), false)) {
										if (!result_set.contains(h)) {
											result_set.add(h);
										}
									}
								}
							}
						}
					} else if (sql._strOperator == "<=") {
						for (Hashtable<String, Object> h : records) {
							Enumeration<String> enumeration = h.keys();
							while (enumeration.hasMoreElements()) {
								String key = enumeration.nextElement();
								if (key.equals(sql._strColumnName)) {
									if (Compare2((sql._objValue), h.get(key), true)) {
										if (!result_set.contains(h)) {
											result_set.add(h);
										}
									}
								}
							}
						}
					} else if (sql._strOperator == "!=") {
						for (Hashtable<String, Object> h : records) {
							Enumeration<String> enumeration = h.keys();
							while (enumeration.hasMoreElements()) {
								String key = enumeration.nextElement();
								if (key.equals(sql._strColumnName)) {
									if (!(h.get(key).equals(sql._objValue))) {
										if (!result_set.contains(h)) {
											result_set.add(h);
										}
									}
								}
							}
						}
					}

				}
			}
		} else if (Operator == "AND") {
			for (Page p : pageListOfFirstOperant) {
				Vector<Hashtable<String, Object>> records = deserializePage(p.path);
				for (Hashtable<String, Object> h : records) {
					int counter = 0;
					Enumeration<String> enumeration = h.keys();
					while (enumeration.hasMoreElements()) {
						String key = enumeration.nextElement();

						for (SQLTerm sql : sqlTerms) {
							if (sql._strOperator == "=") {
								if (sql._strColumnName.equals(key)) {
									if (h.get(key).equals(sql._objValue)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == ">") {
								if (sql._strColumnName.equals(key)) {
									if (Compare2(h.get(key), (sql._objValue), false)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == ">=") {
								if (sql._strColumnName.equals(key)) {
									if (Compare2(h.get(key), (sql._objValue), true)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == "<") {
								if (sql._strColumnName.equals(key)) {
									if (Compare2((sql._objValue), h.get(key), false)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == "<=") {
								if (sql._strColumnName.equals(key)) {
									if (Compare2((sql._objValue), h.get(key), true)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == "!=") {
								if (sql._strColumnName.equals(key)) {
									if (!(h.get(key).equals(sql._objValue))) {
										counter++;
										break;
									}
								}
							}

						}
					}
					if (counter == sqlTerms.length) {
						result_set.add(h);
					}
				}
			}
		} else if (Operator == "XOR") {
			for (Page p : pageListOfFirstOperant) {
				Vector<Hashtable<String, Object>> records = deserializePage(p.path);
				for (Hashtable<String, Object> h : records) {
					int counter = 0;
					Enumeration<String> enumeration = h.keys();
					while (enumeration.hasMoreElements()) {
						String key = enumeration.nextElement();

						for (SQLTerm sql : sqlTerms) {
							if (sql._strOperator == "=") {
								if (sql._strColumnName.equals(key)) {
									if (h.get(key).equals(sql._objValue)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == ">") {
								if (sql._strColumnName.equals(key)) {
									if (Compare2(h.get(key), (sql._objValue), false)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == ">=") {
								if (sql._strColumnName.equals(key)) {
									if (Compare2(h.get(key), (sql._objValue), true)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == "<") {
								if (sql._strColumnName.equals(key)) {
									if (Compare2((sql._objValue), h.get(key), false)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == "<=") {
								if (sql._strColumnName.equals(key)) {
									if (Compare2((sql._objValue), h.get(key), true)) {
										counter++;
										break;
									}
								}
							} else if (sql._strOperator == "!=") {
								if (sql._strColumnName.equals(key)) {
									if (!(h.get(key).equals(sql._objValue))) {
										counter++;
										break;
									}
								}
							}

						}
					}
					if (counter < sqlTerms.length && counter != 0) {
						result_set.add(h);
					}
				}
			}
		}
		return result_set.iterator();
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

	public static void serializeGrid(String tableName, GridIndex grid, int gridCount) {
		try {

			FileOutputStream fileOut = new FileOutputStream(
					dataPath + "/" + tableName + "/GridFolder" + "/GridIndex" + (gridCount) + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(grid);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static GridIndex deserializeGrid(String tableName, int gridCount) {
		GridIndex grid = null;
		try {

			FileInputStream fileIn = new FileInputStream(
					dataPath + "/" + tableName + "/GridFolder" + "/GridIndex" + gridCount + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			grid = (GridIndex) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {

			c.printStackTrace();

		}
		return grid;
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

	@SuppressWarnings("unchecked")
	public static Vector<Hashtable<String, String[]>> deserializeGridList(String tableName) {

		Vector<Hashtable<String, String[]>> pageList = null;
		try {
			FileInputStream fileIn = new FileInputStream(
					dataPath + "/" + tableName + "/GridFolder" + "/GridList.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			pageList = (Vector<Hashtable<String, String[]>>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {

			c.printStackTrace();

		}
		return pageList;

	}

	public static void serializeGridList(String path, Vector<Hashtable<String, String[]>> v) {
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

	public static boolean Compare2(Object lastEntry, Object newEntry, boolean equal) throws DBAppException {
		int res;
		if (newEntry instanceof String) {
			res = ((String) newEntry).compareToIgnoreCase((String) lastEntry);
			if (res == 0)
				return equal;
			return res < 0;
		} else if (newEntry instanceof Integer) {
			if (((int) newEntry) == ((int) lastEntry))
				return equal;
			return ((int) newEntry) < ((int) lastEntry);
		} else if (newEntry instanceof Double) {
			res = new BigDecimal((double) newEntry).compareTo(new BigDecimal((double) lastEntry));
			if (res == 0)
				return equal;
			return res < 0;
		} else if (newEntry instanceof Date) {
			res = ((Date) newEntry).compareTo((Date) lastEntry);
			if (res == 0)
				return equal;
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
			int[] minInt = { Integer.parseInt(minArr[0]) - 1900, Integer.parseInt(minArr[1]) - 1,
					Integer.parseInt(minArr[2]) };
			Date min2 = new Date(minInt[0], minInt[1], minInt[2]);
			String[] maxArr = max.split("-");
			int[] maxInt = { Integer.parseInt(maxArr[0]), Integer.parseInt(maxArr[1]), Integer.parseInt(maxArr[2]) };
			Date max2 = new Date(maxInt[0] - 1900, maxInt[1] - 1, maxInt[2]);
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
			if ((value2.after(min2) && value2.before(max2)) || value2.compareTo(max2) == 0
					|| value2.compareTo(min2) == 0)
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

//	public static void main(String[] args) throws DBAppException {
	// (new DBApp()).createIndex("students",new String[] {"gpa","first_name"});

//		GridIndex g = deserializeGrid("students", 0);
//		g.printGrid();
//		Vector<Bucket> v =g.get(new int[] { 0, 0 });
//		System.out.println(v.size());
//		String s = "";
//		s.toLowerCase();
//		v.add(new Bucket(2));
//		// System.out.println(v);
//		g.set(new int[] { 0, 0 }, v);
//		g.printGrid();
	// System.out.println(g.get(new int[] {0,0,0}));
//	}

	public static void main(String[] args) {
		DBApp db = new DBApp();
		
//		 Hashtable<String, Object> ht = new Hashtable<String, Object>();
//
//
//	        ht.put("first_name", "kjzLUp" );
//	        ht.put("last_name", "dYOjWh" );
//
//	        try {
//	            db.deleteFromTable("students", ht);
//	        } catch (DBAppException e) {
//	            // TODO Auto-generated catch block
//	            e.printStackTrace();
//	        }
		
//	 Vector<Hashtable<String,Object>> vec = db.deserializePage(dataPath + "/students/Page1.class");
//	 
//	 for(Hashtable<String,Object> htbl : vec) {
//		 Enumeration<String> e = htbl.keys();
//	        
//	        while(e.hasMoreElements()) {
//	        	String key = e.nextElement();
//	        	System.out.println(key + " " + htbl.get(key));	        		 
//	        }
//	        System.out.println();
//	        
//	 }

//	 			String[] s1 = {"GUC", "MUE", "MUN", "MUG"};
//	 			String[] s2 = {"MUE", "MUN", "GUC", "MUG", "GAGA"};
//	 			System.out.println(db.arraysAreEqual(s1,s2));
//					String tableName = "students";
//		
//			        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
//			        htblColNameType.put("id", "java.lang.String");
//			        htblColNameType.put("first_name", "java.lang.String");
//			        htblColNameType.put("last_name", "java.lang.String");
//			        htblColNameType.put("dob", "java.util.Date");
//			        htblColNameType.put("gpa", "java.lang.Double");
//		
//			        Hashtable<String, String> minValues = new Hashtable<>();
//			        minValues.put("id", "43-0000");
//			        minValues.put("first_name", "AAAAAA");
//			        minValues.put("last_name", "AAAAAA");
//			        minValues.put("dob", "1990-01-01");
//			        minValues.put("gpa", "0.7");
//		
//			        Hashtable<String, String> maxValues = new Hashtable<>();
//			        maxValues.put("id", "99-9999");
//			        maxValues.put("first_name", "zzzzzz");
//			        maxValues.put("last_name", "zzzzzz");
//			        maxValues.put("dob", "2000-12-31");
//			        maxValues.put("gpa", "5.0");
//		
//			        try {
//						db.createTable(tableName, "id", htblColNameType, minValues, maxValues);
//					} catch (DBAppException e1) {
//						// TODO Auto-generated catch block
//						e1.printStackTrace();
//					}

//		String[] strArr = { "id"};
//		try {
//			db.createIndex("students", strArr);
//		} catch (DBAppException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

//					GridIndex grid = db.deserializeGrid("students", 1);
//					int[] ar = {0};
//					//String s = "wa7ed";
//					//grid.set(ar, s);
//					grid.printGrid();
//					//int[] ar = {0,0};
//			        Vector<Bucket> vecBucks = (Vector<Bucket>)grid.get(ar);
//			        System.out.println(vecBucks);
//			        
//			        Vector<Hashtable<String,String[]>> gridIndices = db.deserializeGridList("students");
//			        
//			        Hashtable<String,String[]> ht = gridIndices.get(0);
//			        Enumeration<String> e = ht.keys();
//			        
//			        while(e.hasMoreElements()) {
//			        	String key = e.nextElement();
//			        	System.out.print(key + " ");
//			        	
//			        	for(String s : ht.get(key)) {
//			        		System.out.print(s + " ");
//			        	}
//			        	
//			        }
//			        

		// System.out.println(gridIndices.size());
	}

}