import java.io.Serializable;

public class SQLTerm implements Serializable {

    String strTableName;
    String strColumnName;
    String strOperator;
    Object objValue;

    public SQLTerm(String strTableName, String strColumnName,String strOperator,Object objValue)
    {
        this.strColumnName = strColumnName;
        this.strTableName = strTableName;
        this.strOperator = strOperator;
        this.objValue = objValue;
    }
    
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	return this.strColumnName +" " + this.strTableName + " " + this.strOperator + " " + this.objValue.toString();
    }
    
	public String getStrTableName() {
		return strTableName;
	}

	public void setStrTableName(String strTableName) {
		this.strTableName = strTableName;
	}

	public String getStrColumnName() {
		return strColumnName;
	}

	public void setStrColumnName(String strColumnName) {
		this.strColumnName = strColumnName;
	}

	public String getStrOperator() {
		return strOperator;
	}

	public void setStrOperator(String strOperator) {
		this.strOperator = strOperator;
	}

	public Object getObjValue() {
		return objValue;
	}

	public void setObjValue(Object objValue) {
		this.objValue = objValue;
	}
}