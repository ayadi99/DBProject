import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

class Page implements Serializable{
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
		return this.path;
	}
}