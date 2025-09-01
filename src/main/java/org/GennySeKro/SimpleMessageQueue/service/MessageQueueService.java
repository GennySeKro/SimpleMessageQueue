package org.GennySeKro.SimpleMessageQueue.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.GennySeKro.SimpleMessageQueue.dao.MessageStorage;
import org.GennySeKro.SimpleMessageQueue.model.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @ClassName: MessageQueueService
 * @Description: 业务服务层：消息队列核心服务
 *               实现消息队列的核心业务逻辑，依赖DAO层进行数据持久化
 * Author: genny
 * Date: 2025/8/29 14:41
 **/

@Slf4j
@Service
public class MessageQueueService {

    /*
    主题到消息队列的映射
     */
    private final Map<String, BlockingQueue<Message>> toppicQueues = new ConcurrentHashMap<>();

    /*
    读写锁保护主题队列的并发访问
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /*
    待确认消息缓存
     */
    private final Map<String, Message> pendingAckMessage = new ConcurrentHashMap<>();

    /*
    定时任务线程池，处理超时未确认消息
     */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    /*
    依赖注入：业务层依赖数据访问层
     */
    private final MessageStorage messageStorage;

    @Autowired
    public MessageQueueService(MessageStorage messageStorage){
        this.messageStorage = messageStorage;
    }

    /*
    初始化：系统启动时执行
     */
    @PostConstruct
    public void init() {
        log.info("初始化消息队列服务...");

        //从持久层加载数据
        List<Message> messageList = messageStorage.loadAllMessage();
        messageList.forEach(msg -> {
            if (!isExpired(msg)) {
                toppicQueues.computeIfAbsent(msg.getTopic(), k -> new LinkedBlockingDeque<>())
                        .offer(msg);
            }else {
                messageStorage.deleteMessage(msg.getId());
            }
        });
        log.info("初始化完成，加载{}条有效信息", messageList.size());

        // 启动定时任务，每30秒检查一次超时未确认消息
        scheduler.scheduleAtFixedRate(this::handleExpiredAcks, 30, 30, TimeUnit.SECONDS);
    }

    /*
    创建主题
     */
    public void createTopic(String topic) {
        lock.writeLock().lock();
        try {
            if (!toppicQueues.containsKey(topic)){
                toppicQueues.put(topic, new LinkedBlockingDeque<>());
                log.info("创建主题：{}", topic);
            }
        }finally {
            lock.writeLock().unlock();
        }
    }

    /*
    发送消息
     */
    public void sendMessage(String topic, String content, long ttl){
        lock.readLock().lock();
        try {
            BlockingQueue<Message> queue = toppicQueues.get(topic);
            if (queue == null){
                throw new IllegalArgumentException("主题不存在：" + topic);
            }

            Message message = new Message(topic, content, ttl);
            queue.offer(message);
            log.info("消息[{}] 发送到主题[{}]", message.getId(), topic);
        }finally {
            lock.readLock().unlock();
        }
    }

    /*
    接收消息
     */
    public Message receiveMessage(String topic, long timeout, TimeUnit unit, boolean autoAck) throws InterruptedException {
        BlockingQueue<Message> queue = toppicQueues.get(topic);
        if (queue == null){
            throw new InterruptedException("主题不存在：" + topic);
        }

        Message message = queue.poll(timeout, unit);
        if (message == null){
            return null;
        }

        //检查消息是否过期
        if (isExpired(message)){
            messageStorage.deleteMessage(message.getId());
            log.info("消息[{}]已过期，启动丢弃", message.getId());
            return null;
        }

        //记录接收时间
        message.setReceiveTime(System.currentTimeMillis());

        //非自动确认则加入待确认缓存
        if (!autoAck){
            pendingAckMessage.put(message.getId(), message);
        }else {
            //自动确认直接删除持久化数据
            messageStorage.deleteMessage(message.getId());
        }

        log.info("消息[{}]被消费，主题[{}]",message.getId(), topic);
        return message;
    }

    //确认消息
    public void acknowledgeMessage(String messageId){
        Message message = pendingAckMessage.remove(messageId);
        if (message != null){
            messageStorage.deleteMessage(messageId);
            log.info("消息 [{}] 已经确认", messageId);
        }
    }

    //处理超时未确认的消息
    private void handleExpiredAcks(){
        long now = System.currentTimeMillis();
        List<Message> expiredMessage = new ArrayList<>();

        //找出超时未确认消息
        pendingAckMessage.values().forEach(msg -> {
            if (now - msg.getReceiveTime() > 6_000_0){
                expiredMessage.add(msg);
            }
        });

        //重新入队并从待确认缓存中移除
        expiredMessage.forEach(msg -> {
            log.warn("消息 [{}] 确认超时， 重新入队", msg.getId());
            pendingAckMessage.remove(msg.getId());
            toppicQueues.get(msg.getTopic()).offer(msg);
        });
    }

    //检查消息是否过期
    private boolean isExpired(Message message){
        return message.getTtl() > 0 &&
                System.currentTimeMillis() - message.getTimestamp() > message.getTtl();
    }

    //销毁：关闭系统
    @PreDestroy
    public void shutDown(){
        log.info("关闭消息队列服务...");
        scheduler.shutdown();
    }
}
