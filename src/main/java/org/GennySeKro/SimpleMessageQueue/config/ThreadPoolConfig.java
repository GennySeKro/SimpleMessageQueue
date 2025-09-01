package org.GennySeKro.SimpleMessageQueue.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName: ThreadPoolConfig
 * @Description: 线程池配置类
 *  *            集中管理线程池资源
 * Author: genny
 * Date: 2025/8/29 17:05
 **/
@Configuration
public class ThreadPoolConfig {

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private static final int CORE_POOL_SIZE = Math.max(2, Math.max(CPU_COUNT - 1, 4));

    private static final int MAX_POOL_SIZE = CPU_COUNT * 2 + 1;

    private static final int QUEUE_CAPACITY = 100;

    private static final long KEEP_ALIVE_SECONDS = 60L;

    /*
    生产者线程池
     */
    @Bean(name = "producerThreadPool")
    public ExecutorService producer() {
        return new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(QUEUE_CAPACITY),
                new ThreadFactory() {

                    private final AtomicInteger counter = new AtomicInteger();

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "producer-thread-" + counter.getAndIncrement());
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy() //队列满时让提交任务的线程执行任务
        );
    }

    /**
     * 消费者线程池
     */
    @Bean(name = "consumerThreadPool")
    public ExecutorService consumerThreadPool() {
        return new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "consumer-thread-" + counter.getAndIncrement());
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}
