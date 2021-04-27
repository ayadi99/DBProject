import java.util.Hashtable;
import java.util.Vector;

public class test {

	public static void main(String[] args) {
		
		Vector<Hashtable<String, Integer>> vec = new Vector<Hashtable<String,Integer>>();
		
		
		
		Hashtable<String, Integer> record5 = new Hashtable<String, Integer>();
		record5.put("gpa", 1);

		record5.put("id", 3);
		vec.add(record5);
		
		Hashtable<String, Integer> record1 = new Hashtable<String, Integer>();
		record1.put("gpa", 1);
		record1.put("id", 2);
		vec.add(record1);

		Hashtable<String, Integer> record3 = new Hashtable<String, Integer>();
		record3.put("gpa", 1);
		record3.put("id", 1);
		vec.add(record3);
		
		Hashtable<String, Integer> record4 = new Hashtable<String, Integer>();
		record4.put("gpa", 11);
		record4.put("id", 4);
		vec.add(record4);
		
		Hashtable<String, Integer> record2 = new Hashtable<String, Integer>();
		record2.put("gpa", 151);
		record2.put("id", 5);
		vec.add(record2);
		
		System.out.println(vec.size());
		
		String primaryKey = "id";
		System.out.println(vec.toString());
		
		boolean sorted = false;
	    Hashtable<String, Integer> temp;
	    while(!sorted) {
	        sorted = true;
	        for (int i = 0; i < vec.size() - 1; i++) {
	            if (vec.get(i).get(primaryKey) > vec.get(i+1).get(primaryKey)) {
	                temp = (Hashtable<String, Integer>) vec.get(i);
	                vec.remove(temp);
	                vec.add(i+1,temp);
	                sorted = false;
	            }
	        }
	    }
	    
	    System.out.println(vec.size());
		
		
		System.out.println(vec.toString());

	}

}
