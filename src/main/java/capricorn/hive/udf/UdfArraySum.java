package capricorn.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * @Description : 数组求和（可作为复杂udf示例参考）
 * @Author : Capricorn.QBB
 * @Date : 2019-08-20
 * @Version : 1.0
 */
@Description(name = "array_sum",
		value = "_FUNC_(array) - return summary of array, support type < String,Int,Double >")
public class UdfArraySum extends GenericUDF {

	private static final Log LOG = LogFactory.getLog(UdfArraySum.class.getName());
	private ListObjectInspector arrayOI;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 1) {
			throw new UDFArgumentLengthException("this udf only takes 1 arg : List<T> t");
		}

		ObjectInspector list = arguments[0];
		if (!(list instanceof ListObjectInspector)) {
			throw new UDFArgumentException("arg type must be a list");
		}

		this.arrayOI = (ListObjectInspector) list;
		if (!(arrayOI.getListElementObjectInspector() instanceof StringObjectInspector) ||
				!(arrayOI.getListElementObjectInspector() instanceof IntObjectInspector) ||
				!(arrayOI.getListElementObjectInspector() instanceof DoubleObjectInspector)) {
			throw new UDFArgumentException("arg type must be a list, support elements type < String,Int,Double >");
		}

		return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object array = arguments[0].get();
		Double sum = 0.0;

		int arrayLength = this.arrayOI.getListLength(array);
		if (arrayLength <= 0) {
			return sum;
		}

		for (int i = 0; i < arrayLength; ++i) {
			try {
				Object element = this.arrayOI.getListElement(array, i);
				Double d = Double.valueOf(element.toString());
				sum += d;
			} catch (NumberFormatException e) {
				LOG.debug("skip this element : " + this.arrayOI.getListElement(array, i) + " e : " + e.getMessage());
			}
		}

		return sum;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "arraySum()";
	}
}
