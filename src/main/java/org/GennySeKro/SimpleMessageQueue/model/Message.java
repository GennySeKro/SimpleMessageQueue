package org.GennySeKro.SimpleMessageQueue.model;

import lombok.Data;

import java.util.UUID;

/**
 * @ClassName: Message
 * @Description: 消息实体类，存储消息的基本信息
 * Author: genny
 * Date: 2025/8/29 11:58
 **/
@Data
public class Message {

    /*
    消息唯一标识
     */
    private final String id;

    /*
    消息所属主题
     */
    private final String topic;

    /*
    消息内容
     */
    private final String content;

    /*
    消息创建时间戳
     */
    private final long timestamp;

    /*
    消息存活时间，0表示永不过期
     */
    private final long ttl;

    /*
    消息被接收的时间戳
     */
    private long receiveTime;

    public Message(String topic, String content, long ttl){
        //todo 自定义分布式 id
        this.id = UUID.randomUUID().toString();
        this.topic = topic;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
        this.ttl = ttl;
    }

    /*
    设置消息被接收的时间
     */
    public void setReceiveTime() {
        this.receiveTime = System.currentTimeMillis();
    }

}
