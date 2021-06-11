import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

public class records implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String path;
	int indexInPage;
	Object primaryKey;
	
	public records(Object primaryKey)
	{
		this.primaryKey = primaryKey;
	}
	
	public records(String path, int indexInPage, Object primaryKey) {
		this.path = path;
		this.indexInPage = indexInPage;
		this.primaryKey = primaryKey;
	}

	public String toString() {
		return "Path is " + path + " and indexInPage: " + indexInPage;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return Compare(this.primaryKey, ((records) obj).primaryKey);
	}

	public static boolean Compare(Object lastEntry, Object newEntry) {
		if (newEntry instanceof String)
			return ((String) newEntry).equals((String) lastEntry);
		else if (newEntry instanceof Integer)
			return ((int) newEntry) == ((int) lastEntry);
		else if (newEntry instanceof Double)
			return new BigDecimal((double) newEntry).equals(new BigDecimal((double) lastEntry));
		else if (newEntry instanceof Date)
			return ((Date) newEntry).equals((Date) lastEntry);
		else 
			return false;
	}

}