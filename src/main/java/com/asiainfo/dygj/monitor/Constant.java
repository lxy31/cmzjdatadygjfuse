package com.asiainfo.dygj.monitor;

import java.util.Map;
import java.util.Properties;

/**
 * @Author Liu xiaoyu
 * @Date 2020/12/12 13:52
 * @Version 1.0
 * @describe:
 */
public class Constant {
    //jc kafka
    public static final String JC_BOOTSTRAP_SERVER = PropertiesUtils.getProperty("jc.bootstrap.server");
    public static final String JC_ZOOKEEPER_CONNECT = PropertiesUtils.getProperty("jc.zookeeper.connect");
    public static final String JC_TOPIC_QX = PropertiesUtils.getProperty("jc.topic.QX");
    public static final String JC_TOPIC_LINE = PropertiesUtils.getProperty("jc.topic.line");
    public static final String JC_TOPIC_LOC = PropertiesUtils.getProperty("jc.topic.loc");
    //zz kafka
    public static final String ZZ_BOOTSTRAP_SERVERS = PropertiesUtils.getProperty("zz.bootstrap.servers");
    public static final String ZZ_SECURITY_PROTOCOL = PropertiesUtils.getProperty("zz.security.protocol");
    public static final String ZZ_KERBEROS_DOMAIN_NAME = PropertiesUtils.getProperty("zz.kerberos.domain.name");
    public static final String ZZ_SASL_KERBEROS_SERVICE_NAME = PropertiesUtils.getProperty("zz.sasl.kerberos.service.name");
    public static final String ZZ_TOPIC = PropertiesUtils.getProperty("zz.topic");
    //shi test
    public static final String SQ_BOOTSTRAP_SERVERS = PropertiesUtils.getProperty("sq.bootstrap.servers");
    public static final String SQ_KERBEROS_DOMAIN_NAME = PropertiesUtils.getProperty("sq.kerberos.domain.name");
    public static final String SQ_TOPIC = PropertiesUtils.getProperty("sq.topic");
    //same config
    public static final String GROUP_ID = PropertiesUtils.getProperty("group.id");
    public static final String TEMP_GROUP_ID = PropertiesUtils.getProperty("temp.group.id");
    public static final String BUFFER_MEMORY = PropertiesUtils.getProperty("buffer.memory");
    public static final String MAX_REQUEST_SIZE = PropertiesUtils.getProperty("max.request.size");
    public static final String MAX_POLL_RECORDS = PropertiesUtils.getProperty("max.poll.records");
    public static final String MAX_PARTITION_FETCH_BYTES = PropertiesUtils.getProperty("max.partition.fetch.bytes");
    public static final String AUTO_OFFSET_RESET = PropertiesUtils.getProperty("auto.offset.reset");
    public static final String RETRIES = PropertiesUtils.getProperty("retries");
    public static final String REQUEST_TIMEOUT_MS = PropertiesUtils.getProperty("request.timeout.ms");
    public static final String RETRY_BACKOFF_MS = PropertiesUtils.getProperty("retry.backoff.ms");
    public static final String BATCH_SIZE = PropertiesUtils.getProperty("batch.size");
    public static final String LINGER_MS = PropertiesUtils.getProperty("linger.ms");
    public static final String ACKS = PropertiesUtils.getProperty("acks");
    //Oracle
    public static final String JDBC_CONNECT_DRIVER_CLASS = PropertiesUtils.getProperty("jdbc_connect_driver_class");
    public static final String JDBC_OWN_ORACLEURL = PropertiesUtils.getProperty("jdbc_own_oracleUrl");
    public static final String ORACLE_NAME = PropertiesUtils.getProperty("oracle_name");
    public static final String ORACLE_PASSWORD = PropertiesUtils.getProperty("oracle_password");
    public static final String JDBC_OTHER_ORACLEURL = PropertiesUtils.getProperty("jdbc_other_oracleUrl");
    public static final String APP_NAME = PropertiesUtils.getProperty("app_name");
    public static final String APP_PASSWORD = PropertiesUtils.getProperty("app_password");
    public static final String JDBC_TEST_ORACLEURL = PropertiesUtils.getProperty("jdbc_test_oracleUrl");
    public static final String ORACLE_TEST_NAME = PropertiesUtils.getProperty("oracle_test_name");
    public static final String ORACLE_TEST_PASSWORD = PropertiesUtils.getProperty("oracle_test_password");
    public static final String JDBC_CONNECT_POOL_MAXSIZE = PropertiesUtils.getProperty("jdbc_connect_pool_maxsize");
    public static final String JDBC_EXEC_BATCH_SIZE = PropertiesUtils.getProperty("jdbc_exec_batch_size");
    //Test Mysql
    public static final String JDBC_DRIVER_CLASS = PropertiesUtils.getProperty("jdbc.driver.class");
    public static final String JDBC_MYSQL_URL = PropertiesUtils.getProperty("jdbc.url");
    public static final String MYSQL_NAME = PropertiesUtils.getProperty("jdbc.username");
    public static final String MYSQL_PASSWORD = PropertiesUtils.getProperty("jdbc.password");
    //Mr Mysql
    public static final String JDBC_DRIVER_CLASSURL = PropertiesUtils.getProperty("jdbc.driver.classUrl");
    public static final String MR_USERNAME = PropertiesUtils.getProperty("mr.username");
    public static final String MR_PASSWORD = PropertiesUtils.getProperty("mr.password");
    //name
    public static final String ORACLE = "ORACLE";
    public static final String APP = "APP";
    public static final String TEST = "TEST";
    public static final String MYSQL = "MYSQL";

    //Kafka properties
    public static Properties getKafkaSourceProperties(String flag, Map<String, String> params) {
        Properties sourceProp = new Properties();
        if (flag.equals("zz")) {
            sourceProp.put("bootstrap.servers", ZZ_BOOTSTRAP_SERVERS);
            sourceProp.put("security.protocol", ZZ_SECURITY_PROTOCOL);
            sourceProp.put("kerberos.domain.name", ZZ_KERBEROS_DOMAIN_NAME);
            sourceProp.put("sasl.kerberos.service.name", ZZ_SASL_KERBEROS_SERVICE_NAME);
        } else if (flag.equals("jc")) {
            sourceProp.put("bootstrap.servers", JC_BOOTSTRAP_SERVER);
            sourceProp.put("zookeeper.connect", JC_ZOOKEEPER_CONNECT);
        }
        sourceProp.put("group.id", params.get("group.id"));
        sourceProp.put("buffer.memory", BUFFER_MEMORY);
        sourceProp.put("max.request.size", MAX_REQUEST_SIZE);
        sourceProp.put("max.poll.records", MAX_POLL_RECORDS);
        sourceProp.put("max.partition.fetch.bytes", MAX_PARTITION_FETCH_BYTES);
        sourceProp.put("auto.offset.reset", AUTO_OFFSET_RESET);
        return sourceProp;
    }

    //Kafka consumer properties
    public static Properties getKafkaProduceProperties(String flag) {
        Properties produceProp = new Properties();
        if (flag.equals("zz")) {
            produceProp.put("bootstrap.servers", ZZ_BOOTSTRAP_SERVERS);
            produceProp.put("security.protocol", ZZ_SECURITY_PROTOCOL);
            produceProp.put("kerberos.domain.name", ZZ_KERBEROS_DOMAIN_NAME);
            produceProp.put("sasl.kerberos.service.name", ZZ_SASL_KERBEROS_SERVICE_NAME);
        } else if (flag.equals("jc")) {
            produceProp.put("bootstrap.servers", JC_BOOTSTRAP_SERVER);
            produceProp.put("zookeeper.connect", JC_ZOOKEEPER_CONNECT);
        }
        produceProp.put("buffer.memory", BUFFER_MEMORY);
        produceProp.put("max.request.size", MAX_REQUEST_SIZE);
        produceProp.put("retries", RETRIES);
        produceProp.put("request.timeout.ms", REQUEST_TIMEOUT_MS);
        produceProp.put("retry.backoff.ms", RETRY_BACKOFF_MS);
        produceProp.put("batch.size", BATCH_SIZE);
        produceProp.put("linger.ms", LINGER_MS);
        produceProp.put("acks", ACKS);
        return produceProp;
    }

    public static Properties getSinkKafkaProperties() {
        Properties sinkProp = new Properties();
        sinkProp.put("bootstrap.servers", SQ_BOOTSTRAP_SERVERS);
        sinkProp.put("kerberos.domain.name", SQ_KERBEROS_DOMAIN_NAME);
        sinkProp.put("sasl.kerberos.service.name", ZZ_SASL_KERBEROS_SERVICE_NAME);
        return sinkProp;
    }

}
