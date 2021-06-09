import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

public class GridIndex implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Object grid;
	private Hashtable<String, Object[]> ranges;
	private TableColumns[] gridContent;
	
	public GridIndex(String tableName, String[] columnNames, TableContent content) throws DBAppException {
		int numOfCol = columnNames.length;
		int[] sizes = new int[numOfCol];

		ranges = new Hashtable<String, Object[]>();
		generateRanges(columnNames, content);
		for (int i = 0; i < sizes.length; i++) 
			sizes[i] = 10;
		
		
		Class<?> type = Vector.class;
		this.grid = Array.newInstance(type, sizes);
		fillArray(grid);
	}

	private void generateRanges(String[] columnNames, TableContent content) throws DBAppException {
		gridContent = new TableColumns[columnNames.length];
		String rangeNames = "";
		int ind = 0;
		for (int i = 0; i < columnNames.length; i++)
			rangeNames += columnNames[i] + " ";
		for (int i = 0; i < content.columns.size(); i++)
			if (rangeNames.contains(content.columns.get(i).name))
				gridContent[ind++] = content.columns.get(i);

		for (int i = 0; i < gridContent.length; i++) {
			switch (gridContent[i].type) {
				case "java.lang.String":
					ranges.put(gridContent[i].name, RangeGenerator.getStringRange(gridContent[i].min,gridContent[i].max));
					break;
				case "java.lang.Integer":
					ranges.put(gridContent[i].name, RangeGenerator.getIntRange(Integer.parseInt(gridContent[i].min),Integer.parseInt(gridContent[i].max)));
					break;
				case "java.lang.Double":
					ranges.put(gridContent[i].name, RangeGenerator.getDoubleRange(Double.parseDouble(gridContent[i].min),Double.parseDouble(gridContent[i].max)));
					break;
				case "java.util.Date":
					ranges.put(gridContent[i].name, RangeGenerator.getDateRange(gridContent[i].min,gridContent[i].max));
					break;
				default:
					throw new DBAppException("Grid Failure");	
			}
		}

	}
	
	public int[] getRecordCoordinates(Hashtable<String, Object> record) 
	{
		// TODO Auto-generated method stub
		int[] res = new int[gridContent.length];
		for (int i = 0; i < gridContent.length; i++) 
			res[i] = RangeGenerator.getBucketIndex(record.get(gridContent[i].name),(Object[]) ranges.get(gridContent[i].name)); 
		
		return res;
	}
	
	@SuppressWarnings("unchecked")
	public Vector<Bucket> get(int[] coord) {
		return (Vector<Bucket>) ((Vector<Bucket>)getValue((Object[]) this.grid, coord)).clone();
	}

	public void set(int[] coord, Object value) {
		setValue((Object[]) this.grid, coord, value);
	}

	public static Object getValue(Object[] array, int[] coordinates) {
		Object result = null;
		if (array == null || coordinates == null || 0 > coordinates[0] || coordinates[0] > array.length) {
			result = null;
		} else {
			int x = coordinates[0];
			if (array[x] instanceof Object[]) {
				int[] c = new int[coordinates.length - 1];
				for (int i = 0; i < c.length; i++) {
					c[i] = coordinates[i + 1];
				}
				result = getValue((Object[]) array[x], c);
			} else {
				result = array[x];
			}
		}
		return result;
	}
	
	public static void main(String[] args) throws DBAppException 
	{
		GridIndex g = new GridIndex("students",new String[] {"id"},new TableContent("students"));
		
		g.printGrid();
		
		
	}
	
	public static void setValue(Object arr, int[] indexPath, Object value) {
		Object[] temp = (Object[]) arr;
		int i = 0;
		while (temp[0].getClass().isArray()) {
			temp = (Object[]) temp[indexPath[i]];
			i++;
		}
		temp[indexPath[i]] = value;

	}

	public void printGrid() {
		Object f = this.grid;
		if (f instanceof Object[]) {
			System.out.println(Arrays.deepToString((Object[]) f));
		}
		else {
			int l = Array.getLength(f);
			for (int i = 0; i < l; ++i) {
				System.out.print(Array.get(f, i) + ", ");
			}
			System.out.println();
		}
	}

	private static void fillArray(Object array) {
		if(Array.get(array, 0) == null)
		{
			Arrays.fill((Object[]) array, new Vector<Bucket>());
			return;
		}
		
	    int length = Array.getLength(array);
	   
	    for (int i = 0; i < length; i++) {
	      fillArray(Array.get(array, i));
	    }
	  }
}
