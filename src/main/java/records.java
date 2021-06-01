public class records {
	String path="";
	int indexInPage;
	public records(String path, int indexInPage) {
		this.path = path;
		this.indexInPage = indexInPage;
	}
	@Override
	public String toString() {
		return "Path is "+path+" and indexInPage: "+ indexInPage;
	}
}