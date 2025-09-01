package org.GennySeKro.SimpleMessageQueue.consumer;

import lombok.extern.slf4j.Slf4j;
import org.GennySeKro.SimpleMessageQueue.model.Message;
import org.GennySeKro.SimpleMessageQueue.service.MessageQueueService;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: MessageConsumer
 * @Description: 消费者组件：从队列获取并处理消息 依赖服务层获取消息
 * Author: genny
 * Date: 2025/8/29 14:36
 **/

@Slf4j
public class MessageConsumer implements Runnable{

    private final MessageQueueService messageQueueService;

    private final String consumerName;

    private final String topic;

    private final boolean autoAck;

    private volatile boolean running = true;

    public MessageConsumer(MessageQueueService messageQueueService, String consumerName,
                           String topic, boolean autoAck){
        this.messageQueueService = messageQueueService;
        this.consumerName = consumerName;
        this.topic = topic;
        this.autoAck = autoAck;
    }


    @Override
    public void run() {
        log.info("消费者 [{}] 开始运行， 订阅主题 [{}]，熊确认：{}",
                consumerName, topic, autoAck);

        try {
            while (running) {
                //从队列获取消息， 超时 1 秒
                Message message = messageQueueService.receiveMessage(topic, 1, TimeUnit.SECONDS, autoAck);
                if (message != null){
                    //处理消息
                    processMessage(message);

                    //手动确定模式下，处理完成后确认消息
                    if (!autoAck){
                        messageQueueService.acknowledgeMessage(message.getId());
                    }
                }
            }
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            log.info("消费者 [{}] 被中断", consumerName, e);
        }catch (Exception e){
            log.error("消费者 [{}] 发生错误", consumerName);
        }

        log.info("消费者 [{}] 停止运行", consumerName);
    }

    //处理消息
    private void processMessage(Message message) throws InterruptedException {
        log.info("消费者 [{}] 处理消息 [{}]: {}",
                consumerName, message.getId(), message.getContent());

        //模拟处理耗时
        Thread.sleep((long) (Math.random() * 800));
    }

    public void stop(){
        running = false;
    }
}
