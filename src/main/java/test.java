import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;
import java.util.*;

public class test {

	public static void main(String[] args) throws ParseException {
		 
		  Hashtable<String, Object> record = new Hashtable<String, Object>();

		  Hashtable<String, Object> part = new Hashtable<String, Object>();
		  
		  record.put("id", 1);
		  record.put("gpa", 3.2);
		  record.put("age",13);
		  
		  part.put("age", 13);
		  
		  Set<Entry<String, Object>> e1 = record.entrySet();
		  Set<Entry<String, Object>> e2 = part.entrySet();
		   
		  System.out.println(e1.containsAll(e2));
	}

}
