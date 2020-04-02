package capricorn.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description : 逗号分割示例
 * @Author : Capricorn.QBB
 * @Date : 2020-04-02
 * @Version : 1.0
 */
@Description(name = "split words")
public class UdtfSplit extends GenericUDTF {

	private List<String> datas = new ArrayList<>();

	@Override
	public void process(Object[] objects) throws HiveException {
		String data = objects[0].toString();
		String split = objects[1].toString();
		String[] words = data.split(split);
		for (String word : words) {
			datas.clear();
			datas.add(data);
			forward(datas);
		}
	}

	@Override
	public void close() throws HiveException {
	}

	// 定义输出数据的列名和输出类型
	@Override
	public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
		List<String> filedNames = new ArrayList<>();
		filedNames.add("split_word");

		List<ObjectInspector> filedIos = new ArrayList<>();
		filedIos.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		//		return super.initialize(argOIs);
		return ObjectInspectorFactory.getStandardStructObjectInspector(filedNames, filedIos);
	}
}
