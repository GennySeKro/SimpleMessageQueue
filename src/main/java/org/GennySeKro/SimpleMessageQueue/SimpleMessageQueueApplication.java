package org.GennySeKro.SimpleMessageQueue;

import lombok.extern.slf4j.Slf4j;
import org.GennySeKro.SimpleMessageQueue.consumer.MessageConsumer;
import org.GennySeKro.SimpleMessageQueue.producer.MessageProducer;
import org.GennySeKro.SimpleMessageQueue.service.MessageQueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 应用启动类
 * 组织和协调各个组件运行
 */
@Slf4j
@SpringBootApplication
public class SimpleMessageQueueApplication implements CommandLineRunner {

    @Autowired
    private MessageQueueService messageQueueService;

    @Autowired
    @Qualifier("producerThreadPool")
    private ExecutorService producerThreadPool;

    @Autowired
    @Qualifier("consumerThreadPool")
    private ExecutorService consumerThreadPool;

	public static void main(String[] args) {
		SpringApplication.run(SimpleMessageQueueApplication.class, args);
	}

    @Override
    public void run(String... args) throws Exception {
        //创建测试主题
        messageQueueService.createTopic("order");
        messageQueueService.createTopic("log");

        //创建并提交生产者任务到线程池
        producerThreadPool.submit(new MessageProducer(
                messageQueueService, "订单生产者 A", "order", 0, 10));
        producerThreadPool.submit(new MessageProducer(
                messageQueueService, "订单生产者 B", "order", 0, 8));
        producerThreadPool.submit(new MessageProducer(
                messageQueueService, "日志生产者", "log", 15000, 12));

        //创建并提交消费者任务到线程池
        MessageConsumer orderConsumer1 = new MessageConsumer(
                messageQueueService, "订单消费者 1", "order", true);
        MessageConsumer orderConsumer2 = new MessageConsumer(
                messageQueueService, "订单消费者 2", "order", false);
        MessageConsumer logConsumer = new MessageConsumer(
                messageQueueService, "日志消费者", "log", true);

        consumerThreadPool.submit(orderConsumer1);
        consumerThreadPool.submit(orderConsumer2);
        consumerThreadPool.submit(logConsumer);

        //运行 20s 后结束测试
        Thread.sleep(2_000_0);

        //停止所有消费者
        orderConsumer1.stop();
        orderConsumer2.stop();
        logConsumer.stop();

        //关闭线程池
        shutDownExecutor(producerThreadPool, "生产者");
        shutDownExecutor(consumerThreadPool, "消费者");

        log.info("消息队列测试完成");
    }

    /*
    关闭线程池
     */
    private void shutDownExecutor(ExecutorService executor, String name){
        log.info("关闭 {} 线程池...", name);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)){
                executor.shutdownNow();
            }
        }catch (InterruptedException e){
            executor.shutdownNow();
        }

        log.info("{} 线程池已关闭", name);
    }
}
