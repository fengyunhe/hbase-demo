import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class User2BasicMapReduce extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("user2", scan, ReadUserMapper.class, Text.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("basic", WriteBasicReduce.class, job);
        job.setNumReduceTasks(10);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
