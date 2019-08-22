package capricorn.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.math.BigDecimal;

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
    private IntObjectInspector scale;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1 || arguments.length >= 3) {
            throw new UDFArgumentLengthException("this udf only takes 1 or 2 arg : List<T>, int");
        }

        ObjectInspector list = arguments[0];
        if (!(list instanceof ListObjectInspector)) {
            throw new UDFArgumentException("arg type must be a list");
        }

        this.arrayOI = (ListObjectInspector) list;
        if (!(this.arrayOI.getListElementObjectInspector() instanceof StringObjectInspector) &&
                !(this.arrayOI.getListElementObjectInspector() instanceof IntObjectInspector) &&
                !(this.arrayOI.getListElementObjectInspector() instanceof DoubleObjectInspector) &&
                !(this.arrayOI.getListElementObjectInspector() instanceof HiveDecimalObjectInspector)) {
            throw new UDFArgumentException("arg type must be a list, support elements type < String,Int,Double >");
        }

        if (arguments.length == 2) {
            scale = (IntObjectInspector) arguments[1];
        }

        return PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object array = arguments[0].get();
//        HiveDecimal sum;
        BigDecimal sum = new BigDecimal(0);

        int arrayLength = this.arrayOI.getListLength(array);
        if (arrayLength <= 0) {
            return sum;
        }

        for (int i = 0; i < arrayLength; ++i) {
            try {
                Object element = this.arrayOI.getListElement(array, i);
                BigDecimal ev = new BigDecimal(element.toString());
                sum = sum.add(ev);
            } catch (NumberFormatException e) {
                LOG.debug("skip this element : " + this.arrayOI.getListElement(array, i) + " e : " + e.getMessage());
            }
        }

        HiveDecimal result = HiveDecimal.create(sum);
//        HiveDecimal dc = HiveDecimal.create(sum, true);
//        if (this.scale != null) {
//            dc.setScale(this.scale.get(arguments[1].get()), HiveDecimal.ROUND_HALF_EVEN);
//        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "arraySum()";
    }
}
