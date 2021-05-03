import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Objects;
import java.util.Properties;
import java.util.Vector;

public class Table  {
	String TableName;
	int maxPageSize;
	final static String relativePath = "src/main/resources/data/";
	public Table(String TableName ) {
		this.TableName = TableName;
		initPageSize();
		File ParentFile = new File(relativePath);
		
		if(!ParentFile.exists()) {
			ParentFile.mkdir();
		}
		File Table = new File(relativePath+TableName);
		
		if(!Table.exists()) {
			Table.mkdir();
			try {
				Vector<Page> pageList  = new Vector<Page>();
				FileOutputStream f1 = new FileOutputStream(relativePath+this.TableName+"/PageList.class");
				ObjectOutputStream out = new ObjectOutputStream(f1);
				out.writeObject(pageList);
				out.close();
				f1.close();
			} catch (IOException e) {
				e.getMessage();
			}
		}


		

	}

	public  void addPage () {
		try {

			Vector<Hashtable<String, Object>> vec  = new Vector<Hashtable<String, Object>>(maxPageSize);
			
			int NumPage = getPageNum(); 
			FileOutputStream f1 = new FileOutputStream(relativePath+this.TableName+"/Page"+NumPage+".class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(vec);
			out.close();
			f1.close();
		} catch (IOException e) {
			e.getMessage();
		}
	}
	public int getPageNum() {
		return new File(relativePath+TableName).listFiles().length;
	}

	public void deletePage(String path)
	{
		File myObj = new File(path); 
		myObj.delete();
	}
	
	public void updatePageList(Vector<Page> pageList)
	{
		try {
			FileOutputStream f1 = new FileOutputStream(relativePath+this.TableName+"/PageList.class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(pageList);
			out.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	public void initPageSize() {
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
	
	
	
}
