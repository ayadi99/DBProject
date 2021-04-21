
public class TableColumns {
	String name;
	String min;
	String max;
	String type;
	String isCluster;

	public TableColumns(String name,String min,String max,String type, String isCluster) {
		this.name = name;
		this.min = min;
		this.max = max;
		this.type = type;
		this.isCluster = isCluster;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.name +" " + this.min + " " + this.max + " " + this.type + " " + this.isCluster;
	}
}
