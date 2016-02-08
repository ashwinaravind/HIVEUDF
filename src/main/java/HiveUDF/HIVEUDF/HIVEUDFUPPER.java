package HiveUDF.HIVEUDF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
 

public  class HIVEUDFUPPER extends UDF
{
  
	 public String evaluate(final Text s) {
		    if (s == null) { return null; }
		    return new String(s.toString().toUpperCase());
		  }
		}
