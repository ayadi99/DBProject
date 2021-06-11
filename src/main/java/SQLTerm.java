import java.io.Serializable;

public class SQLTerm implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String _strTableName;
	public String _strColumnName;
	public String _strOperator;
	public Object _objValue;
    public SQLTerm()
    {
      
    }
    public SQLTerm(String strTableName, String strColumnName,String strOperator,Object objValue)
    {
        this._strColumnName = strColumnName;
        this._strTableName = strTableName;
        this._strOperator = strOperator;
        this._objValue = objValue;
    }
    
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	return this._strColumnName +" " + this._strTableName + " " + this._strOperator + " " + this._objValue.toString();
    }
    
}