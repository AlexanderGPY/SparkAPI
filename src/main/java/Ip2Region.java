import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.lionsoul.ip2region.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Method;

/**
 * @author: williamzhang@welove-inc.com
 * @date: 2021-06-03
 * @description:
 */
@Description(name = "Ip2Region", value = "_FUNC_(ip) - return the ip address",
        extended = "Example: select _FUNC_('220.248.12.158') from src limit 1;\n"
                + "中国|0|上海|上海市|联通")
public class Ip2Region extends GenericUDF {
    private ObjectInspectorConverters.Converter converter;
    private DbSearcher searcher;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        converter = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        String dbPath = "";

        File file = new File(dbPath);
        if (!file.exists()) {
            System.out.println("Error: Invalid  file");
            return null;
        }
        DbConfig config = null;
        try {
            config = new DbConfig();
            searcher = new DbSearcher(config, dbPath);
        } catch (DbMakerConfigException | FileNotFoundException e) {
            e.printStackTrace();
        }


        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        }
        Text ip = (Text) converter.convert(arguments[0].get());

        //查询算法
        int algorithm = DbSearcher.BTREE_ALGORITHM;
        try {


            Method method = null;
            method = searcher.getClass().getMethod("btreeSearch", String.class);

            DataBlock dataBlock = null;
            if (!Util.isIpAddress(ip.toString())) {
                System.out.println("Error: Invalid ip address");
            }

            dataBlock = (DataBlock) method.invoke(searcher, ip.toString());

            return new Text(dataBlock.getRegion());

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }

}
