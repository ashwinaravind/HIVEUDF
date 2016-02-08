package HiveUDF.HIVEUDF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
public class GenericUDFNvl extends GenericUDF{
	private ReturnObjectInspectorResolver returnOIResolver;
	private ObjectInspector[] argumentsOIs;
	
	/*
	 The initialize method is called first and function parameters are passed  as ObjectInspector
	 ObjectInspector provides a consistent interface for handling complex datatypes
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException{
		 returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		return returnOIResolver.get();
	}
	/*
	 Takes the argument deffered object types from the init function as array and performs the evaluations
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException{		
		Object retval = returnOIResolver.convertIfNecessary(arguments[1].get(),(ObjectInspector) arguments[0]);
		return retval;
	}
	/*
	 This is used within the hadoop task to display debugging info
	 */
	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}
}
