import java.util.Date;

public class RangeGenerator 
{
	
	public static String[] getStringRange(String min,String max)
	{
		return (new StringRange()).range(min, max);
	}
	
	public static int[] getIntRange(int	min,int max)
	{
		int[] res = new int[11];
		res[0] = min;
		res[10] = max;
		int partitionSize = (max-min)/10;
		for (int i = 1; i < res.length-1; i++) {
			min += partitionSize;
			res[i] = min;
		}
		
		return res;
	}
	
	public static double[] getDoubleRange(double	min,double max)
	{
		double[] res = new double[11];
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
}
