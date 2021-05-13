package com.asiainfo.dygj.monitor.flink;

/**
 * @ClassName JdbcBean
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/4/6 18:06
 * @Version 1.0
 **/
public class JdbcBean {
    private String topic = "";
    private String count = "";

    public JdbcBean() {
    }

    public JdbcBean(String topic, String count) {
        this.topic = topic;
        this.count = count;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "JdbcBea{" +
                "topic='" + topic + '\'' +
                ", count=" + count +
                '}';
    }
}
