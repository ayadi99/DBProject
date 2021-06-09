import java.lang.reflect.*;
import java.util.*;

public class test {

	
	public static void main(String[] args) {
		System.out.println(Arrays.toString(new String[] {"hi","i","am"}));
		String[] s = {"yousef","abc"};
		Arrays.sort(s);
		System.out.println(Arrays.toString(s));
	}
//	public static void main(String[] args) {
//	    Class<?> type = Vector.class; // this varies (I receive this)
//	    int[] sizes = new int[] { 3, 3,3,3 }; // this varies too, it could be e.g.
//	                                    // int[]{0} for a scalar
//	    Object f = Array.newInstance(type, sizes);
//	    set(f, new int[] { 0,0,0, 2 },new Vector<Bucket>());
//	    if (f instanceof Object[])
//	        System.out.println(Arrays.deepToString((Object[]) f));
//	    else {
//	        int l = Array.getLength(f);
//	        for (int i = 0; i < l; ++i) {
//	            System.out.print(Array.get(f, i) + ", ");
//	        }
//	        System.out.println();
//	    }
//	    Bucket b = new Bucket(100);
//	    records rec = new records("/", 0); 
//	    b.push(rec);
//	    ((Vector)(getValue((Object[])f, new int[] {0,0,0,2}))).add(b);
//	    System.out.println((getValue((Object[])f, new int[] {0,0,0,2})));
//	    if (f instanceof Object[])
//	        System.out.println(Arrays.deepToString((Object[]) f));
//	    else {
//	        int l = Array.getLength(f);
//	        for (int i = 0; i < l; ++i) {
//	            System.out.print(Array.get(f, i) + ", ");
//	        }
//	        System.out.println();
//	    }
//	}
	
	public static Object getValue(Object[] array, int[] coordinates) {
	    Object result = null;
	    if (array == null || coordinates == null || 0 > coordinates[0]||coordinates[0] > array.length) {
	        result = null;
	    } else {
	        int x = coordinates[0];
	        if (array[x] instanceof Object[]) {
	            int[] c = new int[coordinates.length-1];
	            for(int i = 0; i < c.length; i++) { c[i] = coordinates[i + 1]; }
	            result = getValue((Object[]) array[x], c);
	        } else {
	            result = array[x];
	        }
	    }
	    return result;
	}
	
	public static void set(Object arr, int[] indexPath, Object value) {
	    if (arr instanceof Object[]) {
	        Object[] temp= (Object[]) arr;
	        for (int i = 0; i < indexPath.length - 2; ++i) {
	            temp = (Object[]) temp[indexPath[i]];
	        }
	        Array.set(temp[indexPath[indexPath.length - 2]],
	            indexPath[indexPath.length - 1], value);
	    } else {
	        Array.set(arr,
	                indexPath[0], value);
	    }
	}



}
