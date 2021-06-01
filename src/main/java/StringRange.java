import java.util.regex.Pattern;

public class StringRange 
{
	
	public String[] range(String min, String max) 
	{		
		int rangeDifference = convert26(max)-convert26(min);		
		String[] ranges = getRanges(min,rangeDifference);
		ranges[10] = max;
		return ranges;
	}
	
	private static String[] getRanges(String min, int rangeDifference) {
		// TODO Auto-generated method stub
		String[] ranges = new String[11];
		ranges[0] = min;
		int partitions = (int) Math.ceil(rangeDifference/10.0);
		for (int i = 0; i < 9; i++) {
			for (int j = 0; j < partitions; j++) 
				min = next(min);
			ranges[i+1] = min;
		}
		
		return ranges;
	}

	
	public static int convert26(String x)
	{
		int c = 0;
		x = reverseString(x);
		
		char aRef = 'a';
		char ARef = 'A';
		char zRef = '0';
		for (int i = 0, power = 0; i < x.length(); i++,power++) 
		{
			if(Pattern.matches("[A-Z]", x.charAt(i)+""))
				c+= (x.charAt(i)-ARef)*Math.pow(26, power);
			else if(Pattern.matches("[a-z]", x.charAt(i)+""))
				c+= (x.charAt(i)-aRef)*Math.pow(26, power);
			else if(Pattern.matches("[0-9]", x.charAt(i)+""))
				c+= (x.charAt(i)-zRef)*Math.pow(10, power);
			else
				power--;
		}
		
		return c;
	}
	
	public static String reverseString(String str){  
	    StringBuilder sb=new StringBuilder(str);  
	    sb.reverse();  
	    return sb.toString();  
	}
	

	public static String next(String text) {
        // We will not process empty string
        int len = text.length();
        if (len == 0)
            return text;

        // Determine where does the first alpha-numeric starts.
        boolean alphaNum = false;
        int alphaNumPos = -1;
        for (char c : text.toCharArray()) {
            alphaNumPos++;
            if (Character.isDigit(c) || Character.isLetter(c)) {
                alphaNum = true;
                break;
            }
        }

        // Now we go calculate the next successor char of the given text.
        StringBuilder buf = new StringBuilder(text);
        if (!alphaNum || alphaNumPos == 0 || alphaNumPos == len) {
            // do the entire input text
            next(buf, buf.length() - 1, alphaNum);
        } else {
            // Strip the input text for non alpha numeric prefix. We do not need to process these prefix but to save and
            // re-attach it later after the result.
            String prefix = text.substring(0, alphaNumPos);
            buf = new StringBuilder(text.substring(alphaNumPos));
            next(buf, buf.length() - 1, alphaNum);
            buf.insert(0, prefix);
        }

        // We are done.
        return buf.toString();
    }

    /** Internal method to calculate string successor value on alpha numeric chars only. */
    private static void next(StringBuilder buf, int pos, boolean alphaNum) {
        // We are asked to carry over next value for the left most char
        if (pos == -1) {
            char c = buf.charAt(0);
            String rep = null;
            if (Character.isDigit(c))
                rep = "1";
            else if (Character.isLowerCase(c))
                rep = "a";
            else if (Character.isUpperCase(c))
                rep = "A";
            else
                rep = Character.toString((char) (c + 1));
            buf.insert(0, rep);
            return;
        }

        // We are asked to calculate next successor char for index of pos.
        char c = buf.charAt(pos);
        if (Character.isDigit(c)) {
            if (c == '9') {
                buf.replace(pos, pos + 1, "0");
                next(buf, pos - 1, alphaNum);
            } else {
                buf.replace(pos, pos + 1, Character.toString((char)(c + 1)));
            }
        } else if (Character.isLowerCase(c)) {
            if (c == 'z') {
                buf.replace(pos, pos + 1, "a");
                next(buf, pos - 1, alphaNum);
            } else {
                buf.replace(pos, pos + 1, Character.toString((char)(c + 1)));
            }
        } else if (Character.isUpperCase(c)) {
            if (c == 'Z') {
                buf.replace(pos, pos + 1, "A");
                next(buf, pos - 1, alphaNum);
            } else {
                buf.replace(pos, pos + 1, Character.toString((char)(c + 1)));
            }
        } else {
            // If input text has any alpha num at all then we are to calc next these characters only and ignore the
            // we will do this by recursively call into next char in buf.
            if (alphaNum) {
                next(buf, pos - 1, alphaNum);
            } else {
                // However if the entire input text is non alpha numeric, then we will calc successor by simply
                // increment to the next char in range (including non-printable char!)
                if (c == Character.MAX_VALUE) {
                    buf.replace(pos, pos + 1, Character.toString(Character.MIN_VALUE));
                    next(buf, pos - 1, alphaNum);
                } else {
                    buf.replace(pos, pos + 1, Character.toString((char)(c + 1)));
                }
            }
        }
    }
}
