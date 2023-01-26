import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static final String TB_NAME_USER = "user2";

    public static void main(String[] args) throws Exception {
        Configuration conf = getConfiguration();
        createSchemaTables(conf);
        modifyColumns(conf);
        addData(conf);
        listData(conf);

        int exitCode = ToolRunner.run(conf, new User2BasicMapReduce(), args);
        System.exit(exitCode);
    }

    private static void listData(Configuration conf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Scan scan = new Scan().addFamily("info".getBytes()).addFamily("status".getBytes());
            ResultScanner scanner = connection.getTable(TableName.valueOf(TB_NAME_USER))
                    .getScanner(scan);
            for (Result result : scanner) {
                log.info(result.toString());
            }
        }
    }

    private static void addData(Configuration conf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Table demoTable = connection.getTable(TableName.valueOf(TB_NAME_USER));
            byte[] infos = Bytes.toBytes("info");
            byte[] statuses = Bytes.toBytes("status");
            demoTable.put(List.of(
                    new Put("1".getBytes()).addColumn(infos,
                            Bytes.toBytes("address"), Bytes.toBytes("延长县" + RandomUtils.nextLong())),
                    new Put("1".getBytes()).addColumn(infos,
                            Bytes.toBytes("name"), Bytes.toBytes("xiaoming" + RandomUtils.nextLong())),
                    new Put("1".getBytes()).addColumn(infos,
                            Bytes.toBytes("age"), Bytes.toBytes("1")),
                    new Put("1".getBytes()).addColumn(statuses,
                            Bytes.toBytes("live"), Bytes.toBytes("suspend")),
                    new Put("1".getBytes()).addColumn(statuses,
                            Bytes.toBytes("award"), Bytes.toBytes("2023")),

                    new Put("2".getBytes()).addColumn(infos,
                            Bytes.toBytes("address"), Bytes.toBytes("延长县" + RandomUtils.nextLong())),
                    new Put("2".getBytes()).addColumn(infos,
                            Bytes.toBytes("name"), Bytes.toBytes("xiaoming" + RandomUtils.nextLong())),
                    new Put("2".getBytes()).addColumn(infos,
                            Bytes.toBytes("age"), Bytes.toBytes("1")),
                    new Put("2".getBytes()).addColumn(statuses,
                            Bytes.toBytes("live"), Bytes.toBytes("suspend")),

                    new Put("3".getBytes()).addColumn(infos,
                            Bytes.toBytes("address"), Bytes.toBytes("延长县" + RandomUtils.nextLong())),
                    new Put("3".getBytes()).addColumn(infos,
                            Bytes.toBytes("name"), Bytes.toBytes("xiaoming" + RandomUtils.nextLong())),
                    new Put("3".getBytes()).addColumn(infos,
                            Bytes.toBytes("age"), Bytes.toBytes("3")),
                    new Put("3".getBytes()).addColumn(statuses,
                            Bytes.toBytes("live"), Bytes.toBytes("suspend")),
                    new Put("3".getBytes()).addColumn(statuses,
                            Bytes.toBytes("award"), Bytes.toBytes("2023"))
            ));
            demoTable.mutateRow(
                    RowMutations.of(List.of(
                            new Delete("2".getBytes())
                    ))
            );
            deleteCell(demoTable, statuses);
        }
    }

    private static void deleteCell(Table demoTable, byte[] statuses) throws IOException {
        demoTable.delete(new Delete("1".getBytes()).addColumn(statuses, "award".getBytes()));
    }

    private static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        conf.addResource(contextClassLoader.getResource("core-site.xml"));
        conf.addResource(contextClassLoader.getResource("hdfs-site.xml"));
        conf.addResource(contextClassLoader.getResource("hbase-site.xml"));
        return conf;
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TB_NAME_USER))
                    .setColumnFamilies(List.of(
                            ColumnFamilyDescriptorBuilder.of("info")
                    )).build();
            log.info("Creating table. ");
            createOrOverwrite(admin, tableDescriptor);
            createOrOverwrite(admin, TableDescriptorBuilder.newBuilder(TableName.valueOf("basic"))
                    .setColumnFamilies(List.of(
                            ColumnFamilyDescriptorBuilder.of("info")
                    )).build());
            log.info("Creating table Done.");
        }
    }

    public static void modifyColumns(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName user2 = TableName.valueOf(TB_NAME_USER);
            log.info("Modify table. ");
//            admin.disableTable(user2);
            admin.modifyColumnFamily(user2,
                    ColumnFamilyDescriptorBuilder.newBuilder(
                            admin.getDescriptor(user2).getColumnFamily("info".getBytes(StandardCharsets.UTF_8))
                    ).setMaxVersions(5).build());
//            admin.enableTable(user2);
            admin.addColumnFamily(user2, ColumnFamilyDescriptorBuilder.of("status"));
            log.info("Modify table Done. ");
        }
    }

    private static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
}