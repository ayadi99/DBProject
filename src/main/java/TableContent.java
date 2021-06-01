import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class TableContent {
	ArrayList<TableColumns> columns;
	
	public TableContent(String tableName)
	{
		columns = new ArrayList<TableColumns>();
		createContent(tableName);
	}
	
	public void createContent(String tableName) 
	{
		try {
			String[] row;
			boolean endOfTable = false;
			boolean inTable = false;
			
            Path pathToFile = Paths.get("src/main/resources/metadata.csv");
            BufferedReader br = Files.newBufferedReader(pathToFile,StandardCharsets.US_ASCII);
            String line = br.readLine();

            
            while(line!=null) {
                row = line.split(",");
                if(!endOfTable)
                {
                	if(row[0].equals(tableName))
                	{
                		inTable = true;
           
                		this.columns.add(new TableColumns(row[1],row[5],row[6],row[2],row[3]));
                	}
                	else
                	{
                		if(inTable)
                			endOfTable = true;
                	}
                }
                else
                	break;
                
                line = br.readLine();
            }
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	public String getPrimaryKey(String TableName) 
	{
		TableContent c = new TableContent(TableName);
		ArrayList<TableColumns> a =c.columns;
		for(int i =0 ; i < a.size(); i++)
		{
		if(((a.get(i)).isCluster).equals("True"))
		{
			String b = (a.get(i)).name;
			return b;
		}
		
		
		}
		return "";
	}
}
