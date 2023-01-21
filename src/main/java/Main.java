import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final String TABLE_NAME = "user2";
    private static final String CF_DEFAULT = "info";

    public static void main(String[] args) throws IOException {
        Configuration conf = getConfiguration();
        createSchemaTables(conf);
        modifyColumns(conf);
    }

    private static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        conf.addResource(Thread.currentThread().getContextClassLoader().getResource("core-site.xml"));
        conf.addResource(Thread.currentThread().getContextClassLoader().getResource("hdfs-site.xml"));
        conf.addResource(Thread.currentThread().getContextClassLoader().getResource("hbase-site.xml"));
        return conf;
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                    .setColumnFamilies(List.of(
                            ColumnFamilyDescriptorBuilder.of(CF_DEFAULT)
                    )).build();
            log.info("Creating table. ");
            createOrOverwrite(admin, tableDescriptor);
            log.info("Creating table Done.");
        }
    }

    public static void modifyColumns(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName user2 = TableName.valueOf("user2");
            log.info("Modify table. ");
//            admin.disableTable(user2);
            admin.modifyColumnFamily(user2,
                    ColumnFamilyDescriptorBuilder.newBuilder(
                            admin.getDescriptor(user2).getColumnFamily("info".getBytes(StandardCharsets.UTF_8))
                    ).setMaxVersions(5).build());
//            admin.enableTable(user2);
            admin.addColumnFamily(user2,ColumnFamilyDescriptorBuilder.of("status"));
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