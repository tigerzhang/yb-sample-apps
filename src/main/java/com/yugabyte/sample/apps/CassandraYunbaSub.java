package com.yugabyte.sample.apps;

import com.datastax.driver.core.*;
import com.yugabyte.sample.common.YunbaSubLoadGenerator;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class CassandraYunbaSub extends CassandraKeyValue {
    private static final Logger LOG = Logger.getLogger(CassandraYunbaSub.class);

    static {
        // Disable the read-write percentage.
        appConfig.readIOPSPercentage = -1;
        // Set the read and write threads to 1 each.
        appConfig.numReaderThreads = 24;
        appConfig.numWriterThreads = 2;
        // The number of keys to read.
        appConfig.numKeysToRead = -1;
        // The number of keys to write. This is the combined total number of inserts and updates.
        appConfig.numKeysToWrite = -1;
        // The number of unique keys to write. This determines the number of inserts (as opposed to
        // updates).
        appConfig.numUniqueKeysToWrite = NUM_UNIQUE_KEYS;
    }

    private static final String DEFAULT_TABLE_NAME = CassandraYunbaSub.class.getSimpleName() + "2";

    public CassandraYunbaSub() {

    }

    @Override
    public List<String> getCreateTableStatements() {
        return Arrays.asList(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s (" +
                                "appkey varchar," +
                                "id varchar," +
                                "appkey_topic varchar," +
                                "primary key(appkey, id, appkey_topic)) %s;",
                        getTableName(),
                        appConfig.nonTransactionalIndex ? "" : "WITH transactions = { 'enabled' : true }"
                ),
                String.format(
                        "CREATE INDEX IF NOT EXISTS %sByTopic ON %s (appkey_topic) %s;",
                        getTableName(), getTableName(),
                        appConfig.nonTransactionalIndex ?
                                "WITH transactions = { 'enabled' : false, 'consistency_level' : 'user_enforced' }" :
                                "")
        );
    }

    public String getTableName() { return appConfig.tableName != null ? appConfig.tableName : DEFAULT_TABLE_NAME; }

    private PreparedStatement getPreparedSelect() {
        return getPreparedSelect(
                String.format(
                        "SELECT id FROM %s WHERE appkey_topic = ?;",
                        getTableName()),
                appConfig.localReads);
    }

    @Override
    public long doRead() {
//        Key key = getSimpleLoadGenerator().getKeyToRead();
        YunbaSubLoadGenerator.SubData subData = getYunbaSubLoadGenerator().getReadSubData();

        BoundStatement select = getPreparedSelect().bind(subData.appkey + "_" + subData.topic);
        ResultSet rs = getCassandraClient().execute(select);
        List<Row> rows = rs.all();

        LOG.debug("Read " + subData.appkey + " topic " + subData.topic + " size " + rows.size());

        return 1;
    }

    protected PreparedStatement getPreparedInsert() {
        return getPreparedInsert(
                String.format(
                        "INSERT INTO %s (appkey, id, appkey_topic) values" +
                                " (?, ?, ?);",
                        getTableName())
                );
    }

    @Override
    public long doWrite(int threadIdx) {
        YunbaSubLoadGenerator.SubData subData = getYunbaSubLoadGenerator().getWriteSubData();
        try {
            BatchStatement batch = new BatchStatement();
            PreparedStatement insert = getPreparedInsert();
            batch.add(insert.bind(subData.appkey, subData.id,
                    subData.appkey + "_" + subData.topic));
            ResultSet resultSet = getCassandraClient().execute(batch);
            LOG.debug("Wrote keys count: " + "1" + ", return code: " + resultSet.toString());
            getYunbaSubLoadGenerator().recordWriteSuccess(subData.id);
        } catch (Exception e) {
            getYunbaSubLoadGenerator().recordWriteFailure(subData.id);
            throw e;
        }
        return 1;
    }

    @Override
    public List<String> getWorkloadDescription() {
        return Arrays.asList(
                "Workload",
                "Description"
        );
    }

    @Override
    public List<String> getWorkloadOptionalArguments() {
        return Arrays.asList(
                "--num_unique_keys " + appConfig.numUniqueKeysToWrite,
                "--num_reads " + appConfig.numKeysToRead,
                "--num_writes " + appConfig.numKeysToWrite,
                "--num_threads_read " + appConfig.numReaderThreads,
                "--num_threads_write " + appConfig.numWriterThreads,
                "--batch_write",
                "--batch_size " + appConfig.cassandraBatchSize
        );
    }
}
