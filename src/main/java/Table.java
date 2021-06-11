import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.util.Hashtable;
import java.util.Objects;
import java.util.Properties;
import java.util.Vector;

public class Table  {
	String TableName;
	String dataPath = "src/main/resources/data";
	public Table(String TableName ) {
		this.TableName = TableName;
		File ParentFile = new File(dataPath);
		
		if(!ParentFile.exists()) {
			ParentFile.mkdir();
		}
		File Table = new File(dataPath+"/"+TableName);
		
		if(!Table.exists()) {
			Table.mkdir();
			try {
				Vector<Page> pageList  = new Vector<Page>();
				FileOutputStream f1 = new FileOutputStream(dataPath+"/"+this.TableName+"/PageList.class");
				ObjectOutputStream out = new ObjectOutputStream(f1);
				out.writeObject(pageList);
				out.close();
				f1.close();
			} catch (IOException e) {
				e.getMessage();
			}
		}

		

	}
	
	public int maxRowCount() {
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
           return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));

        } catch (IOException e) {
            e.printStackTrace();
        }
		return 0;

	}

	public  void addPage () {
		try {
			Vector<Hashtable<String, Object>> vec  = new Vector<Hashtable<String, Object>>(maxRowCount());
			
			int NumPage = getPageNum(); 
			FileOutputStream f1 = new FileOutputStream(dataPath+"/"+this.TableName+"/Page"+NumPage+".class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(vec);
			out.close();
			f1.close();
		} catch (IOException e) {
			e.getMessage();
		}
	}
	public int getPageNum() {
		return new File(dataPath+"/"+TableName).listFiles().length;
	}

	public void deletePage(String path)
	{
		File myObj = new File(path); 
		myObj.delete();
	}
	
	public void updatePageList(Vector<Page> pageList)
	{
		try {
			FileOutputStream f1 = new FileOutputStream(dataPath+"/"+this.TableName+"/PageList.class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(pageList);
			out.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	
	
}
