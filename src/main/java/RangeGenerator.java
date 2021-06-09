import java.util.Date;

public class RangeGenerator 
{
	
	public static String[] getStringRange(String min,String max)
	{
		return (new StringRange()).range(min, max);
	}
	
	public static Integer[] getIntRange(int	min,int max)
	{
		Integer[] res = new Integer[11];
		res[0] = min;
		res[10] = max;
		int partitionSize = (max-min)/10;
		for (int i = 1; i < res.length-1; i++) {
			min += partitionSize;
			res[i] = min;
		}
		
		return res;
	}
	
	public static Double[] getDoubleRange(double	min,double max)
	{
		Double[] res = new Double[11];
		res[0] = min;
		res[10] = max;
		double partitionSize = (max-min)/10;
		for (int i = 1; i < res.length-1; i++) {
			min += partitionSize;
			res[i] = min;
		}
		
		return res;
	}
	
	public static Date[] getDateRange(String min, String max)
	{
		min = min.replace('-', '/');
		max = max.replace('-', '/');
		
		@SuppressWarnings("deprecation")
		long minTime = (new Date(min)).getTime();
		@SuppressWarnings("deprecation")
		long maxTime = (new Date(max)).getTime();
		
		long partitionSize = (maxTime - minTime)/10;
		Date[] parsedDates = new Date[11];
		parsedDates[0] = new Date(minTime);
		parsedDates[10] = new Date(maxTime);
		
		for (int i = 1; i < parsedDates.length-1; i++) {
			minTime += partitionSize;
			parsedDates[i] = new Date(minTime);
		}
		return parsedDates;
	}
	
		
	public static int getIntBucketIndex(int element, Integer[] ranges)
	{
		int index = -1;
		int lRanges = ranges.length;
		for(int i = 0; i < lRanges - 1; i++) {
			
			
			if(ranges[i] <= element && element <= ranges[i+1]) {
				index = i;
				break;
			}
		}
		
		return index;
	}
	
	public static int getDoubleBucketIndex(Double element, Double[] ranges)
	{
		int index = -1;
		int lRanges = ranges.length;
		for(int i = 0; i < lRanges - 1; i++) {
			if(ranges[i] <= element && element <= ranges[i+1]) {
				index = i;
				break;
			}
		}
		
		return index;
	}
	
	public static int getDateBucketIndex(Date element, Date[] ranges)
	{
		int index = -1;
		int lRanges = ranges.length;
		for(int i = 0; i < lRanges - 1; i++) {
			
			if(element.equals(ranges[i]) || element.equals(ranges[i+1]))
				index = i;
				
			else if(element.after(ranges[i]) && element.before(ranges[i+1])) {
				index = i;
			}
		}
		
		return index;
		
	}
	
	public static int getStringBucketIndex(String element, String[] ranges)
	{
		int index = -1;
		int lRanges = ranges.length;
		
		int elementBase26 = StringRange.convert26(element);
		
		for(int i = 0; i < lRanges - 1; i++) {
			
			int minBase26 = StringRange.convert26(ranges[i]);
			int maxBase26 = StringRange.convert26(ranges[i+1]);
			
			if( minBase26 <= elementBase26 && elementBase26 <= maxBase26) {
				index = i;
				break;
			}
		}
		
		return index;
		
	}
	
	public static int getBucketIndex(Object element, Object[] ranges)
	{
		if(element instanceof Integer)
			return getIntBucketIndex((int)element, (Integer [])ranges);
		
		else if(element instanceof Double) {
			return getDoubleBucketIndex((Double) element,(Double []) ranges);
		}
		
		else if(element instanceof Date) {
			return getDateBucketIndex((Date) element, (Date[]) ranges);
		}
		else if(element instanceof String) {
			return getStringBucketIndex((String) element, (String[]) ranges);
		}
		
		else {
			return -1;
		}
	}
	
	
	
	
}