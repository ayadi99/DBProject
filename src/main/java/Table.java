import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
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
			
		}
		else {
		}
		if(getPageNum()==0) {
			addPage();
		}

	}

	public  void addPage () {
		try {
			Vector vec  = new Vector();
			
			int NumPage = getPageNum()+1; 
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
	
	
}
