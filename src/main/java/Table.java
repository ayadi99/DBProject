import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class Table  {
	String TableName;

	public Table(String TableName ) {
		this.TableName = TableName;
		File ParentFile = new File("tables");
		
		if(!ParentFile.exists()) {
			ParentFile.mkdir();
		}
		File Table = new File("tables/"+TableName);
		
		if(!Table.exists()) {
			Table.mkdir();
			try {
				Vector<Page> pageList  = new Vector<Page>();
				FileOutputStream f1 = new FileOutputStream("tables/"+this.TableName+"/PageList.class");
				ObjectOutputStream out = new ObjectOutputStream(f1);
				out.writeObject(pageList);
				out.close();
				f1.close();
			} catch (IOException e) {
				e.getMessage();
			}
		}
		else {
		}
		

	}

	public  void addPage () {
		try {
			Vector<Hashtable<String, Object>> vec  = new Vector<Hashtable<String, Object>>(200);
			
			int NumPage = getPageNum(); 
			FileOutputStream f1 = new FileOutputStream("tables/"+this.TableName+"/Page"+NumPage+".class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(vec);
			out.close();
			f1.close();
		} catch (IOException e) {
			e.getMessage();
		}
	}
	public int getPageNum() {
		return new File("tables/"+TableName).listFiles().length;
	}

	public void deletePage(int pageNum)
	{
		File myObj = new File("tables/"+this.TableName+"/Page"+pageNum+".class"); 
		myObj.delete();
	}
	
	public void updatePageList(Vector<Page> pageList)
	{
		try {
			FileOutputStream f1 = new FileOutputStream("tables/"+this.TableName+"/PageList.class");
			ObjectOutputStream out = new ObjectOutputStream(f1);
			out.writeObject(pageList);
			out.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
}
