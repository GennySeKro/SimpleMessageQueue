package org.GennySeKro.SimpleMessageQueue.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.GennySeKro.SimpleMessageQueue.model.Message;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * @ClassName: MessageStorage
 * @Description: 数据访问层：消息存储实现
 *  *            仅负责数据的读写操作，不包含业务逻辑
 * Author: genny
 * Date: 2025/8/29 16:48
 **/
@Slf4j
@Component
public class MessageStorage {

    private static final String STORE_DIR = "./mq-storage";

    private final ObjectMapper objectMapper = new ObjectMapper();

    public MessageStorage(){
        //初始化存储目录
        try {
            Files.createDirectories(Paths.get(STORE_DIR));
        }catch (IOException e){
            log.error("初始化存储目录失败", e);
            throw new RuntimeException("无法初始化消息存储");
        }
    }

    //保存消息到文件
    public void saveMessage(Message message){
        try (Writer writer = new FileWriter(getFilePath(message.getId()))){
            objectMapper.writeValue(writer, message);
        }catch (IOException e){
            log.error("保存消息失败：{}", message.getId(), e);
        }
    }

    //加载所有消息
    public List<Message> loadAllMessage(){
        List<Message> messages = new ArrayList<>();

        try (Stream<Path> stream = Files.list(Paths.get(STORE_DIR))){
            stream.filter(Files::isRegularFile)
                    .forEach(path -> {
                        try (Reader reader = new FileReader(path.toFile())){
                            Message message = objectMapper.readValue(reader, Message.class);
                            messages.add(message);
                        }catch (IOException e){
                            log.error("加载消息失败:{}", path.getFileName(), e);
                        }
                    });
        }catch (IOException e){
            log.error("加载消息列表失败", e);
        }

        return messages;
    }

    //删除消息文件
    public void deleteMessage(String messageId){
        try {
            Files.deleteIfExists(Path.of(getFilePath(messageId)));
        }catch (IOException e){
            log.error("删除消息失败：{}", messageId, e);
        }
    }

    //获取消息存储路径
    private String getFilePath(String messageId){
        return STORE_DIR + File.separator + messageId + ".json";
    }

}
