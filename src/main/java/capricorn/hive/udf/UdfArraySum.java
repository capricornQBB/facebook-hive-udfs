package capricorn.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

/**
 * @Description :
 * @Author : Capricorn.QBB
 * @Date : 2019-08-16
 * @Version : 1.0
 */
@Description(name = "array_sum",
        value = "_FUNC_(array) - return summary of array")
public class UdfArraySum extends UDF {
    private static final Log LOG = LogFactory.getLog(UdfArraySum.class.getName());

    public Double evaluate(List<String> list) {
        if (list == null || list.size() == 0) {
            return 0.0;
        }

        double sum = 0;
        for (String o : list) {
            try {
                Double d = Double.valueOf(o);
                sum += d;
            } catch (Exception e) {
                LOG.debug("skip the error");
            }
        }
        return sum;
    }
}
