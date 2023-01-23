import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadUserMapper extends TableMapper<Text, Put> {
    private final Text mapOutputKey = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Mapper<ImmutableBytesWritable, Result, Text, Put>.Context context)
            throws IOException, InterruptedException {
        String rowKey = Bytes.toString(key.get());
        mapOutputKey.set(rowKey);
        Put put = new Put(key.get());
        for (Cell cell : value.rawCells()) {
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                switch (Bytes.toString(CellUtil.cloneQualifier(cell))) {
                    case "name":
                    case "age":
                        put.add(cell);
                        break;
                    default:

                }
            }
        }
        context.write(mapOutputKey, put);
    }
}
