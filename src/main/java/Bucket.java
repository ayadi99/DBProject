import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5154907430954213847L;
	public Vector<records> values;

	public Bucket (int size) {
		values = new Vector<records>(size);
	}
	
	public void push(records rec) {
		values.add(rec);
	}

	public void remove(records rec) {
		values.remove(rec);
	}
	public Vector<records> getRecord() {
		return values;
	}
	public String toString() {
        String str = "";
        for(records rec: values) {
            str = str+rec.toString();
        }
        return str;
      
    }

}


