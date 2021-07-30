
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * @author: williamzhang@welove-inc.com
 * @date: 2021-06-03
 * @description:
 */
public class IPTest {
    public static void main(String[] args) throws HiveException {
        Ip2Region ip2Region = new Ip2Region();
        ip2Region();
    }

    public static void ip2Region() throws HiveException {
        Ip2Region udf = new Ip2Region();
        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector[] init_args = {valueOI0};
        udf.initialize(init_args);
        String ip = "220.248.12.158";

        GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(ip);

        GenericUDF.DeferredObject[] args = {valueObj0};
        Text res = (Text) udf.evaluate(args);
        System.out.println(res);
    }
}
