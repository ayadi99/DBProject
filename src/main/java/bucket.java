import java.util.Vector;

public class bucket {
	
	public Vector<records> values;

	public bucket (int size) {
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
	@Override
	public String toString() {
		String str = "";
		for(records rec: values) {
			str = str+rec.toString();
		}
		return str;
	}
}


