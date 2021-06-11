import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

@SuppressWarnings("serial")
class Page implements Serializable, Comparable<Page>{
	public Object min;
	public Object max;
	public String path;
	public int size;
	public Page(String path) 
	{
		this.path = path;
		size = 0;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.min.toString();
	}

	@Override
	public int compareTo(Page o) {
		return Compare(this.min, o.min);
	}
	
	public static int Compare(Object lastEntry, Object newEntry) {
		// System.out.println(lastEntry);
		if (newEntry instanceof String) 
			return ((String) newEntry).compareToIgnoreCase((String) lastEntry);
		 else if (newEntry instanceof Integer) 
			return ((int) lastEntry)-((int) newEntry);
		 else if (newEntry instanceof Double) 
			return new BigDecimal((double) newEntry).compareTo(new BigDecimal((double) lastEntry));
		 else if (newEntry instanceof Date) 
			return ((Date) newEntry).compareTo((Date) lastEntry);
		
		return 0;
			
	}
	
}