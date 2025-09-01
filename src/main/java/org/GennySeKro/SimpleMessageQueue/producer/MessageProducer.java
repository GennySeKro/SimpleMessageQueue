package org.GennySeKro.SimpleMessageQueue.producer;

import lombok.extern.slf4j.Slf4j;
import org.GennySeKro.SimpleMessageQueue.service.MessageQueueService;

import java.util.Map;

/**
 * @ClassName: MessageProducer
 * @Description: 生产者组件：产生消息并发送到队列
 *  *            依赖服务层完成消息发送
 * Author: genny
 * Date: 2025/8/29 16:03
 **/
@Slf4j
public class MessageProducer implements Runnable{

    private final MessageQueueService messageQueueService;

    private final String producerName;

    private final String topic;

    private final long ttl;

    private final int messageCount;

    private volatile boolean running = true;

    public MessageProducer(MessageQueueService messageQueueService, String producerName,
                           String topic, long ttl, int messageCount){
        this.messageQueueService = messageQueueService;
        this.producerName = producerName;
        this.topic = topic;
        this.ttl = ttl;
        this.messageCount = messageCount;
    }


    @Override
    public void run() {
        log.info("生产者 [{}] 开始运行， 将发送 {} 条消息到主题 [{}]", producerName, messageCount, topic);

        try {
            for (int i = 0; i < messageCount && running; i++){
                String content = String.format("这是来自 %s 的第 %d 条信息", producerName, i + 1);
                messageQueueService.sendMessage(topic, content, ttl);

                //模拟发送间隔
                Thread.sleep((long) (Math.random() * 1000));
            }
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            log.info("生产者 [{}] 被中断", producerName);
        } catch (Exception e){
            log.error("生产者 [{}] 发生错误", producerName, e);
        }

        log.info("生产者 [{}] 完成任务", producerName);
    }

    public void stop() {
        running = false;
    }
}
