package com.chatflow.consumerv3.processor;

import com.chatflow.consumerv3.domain.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for persisting messages to DynamoDB with batching and write-behind pattern.
 * Implements high-throughput writes with configurable batch sizes and flush intervals.
 */
@Service
public class DynamoDB {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDB.class);

  @Value("${dynamodb.table.messages:chatflow-messages}")
  private String messagesTable;

  @Value("${dynamodb.table.participation:chatflow-room-participation}")
  private String participationTable;

  @Value("${dynamodb.table.analytics:chatflow-analytics}")
  private String analyticsTable;

  @Value("${aws.region:us-west-2}")
  private String awsRegion;

  @Value("${dynamodb.batch.size:1000}")
  private int batchSize;

  @Value("${dynamodb.flush.interval.ms:500}")
  private long flushIntervalMs;

  @Value("${dynamodb.writer.threads:4}")
  private int writerThreads;

  private DynamoDbClient dynamoDbClient;
  private ExecutorService writerExecutor;
  private ScheduledExecutorService flushScheduler;

  // Write-behind buffer
  private final BlockingQueue<TaskMessage> writeBuffer = new LinkedBlockingQueue<>(50000);

  // Metrics
  private final AtomicLong messagesWritten = new AtomicLong(0);
  private final AtomicLong writesFailed = new AtomicLong(0);
  private final AtomicLong batchesWritten = new AtomicLong(0);

  private volatile boolean running = false;

  @PostConstruct
  public void init() {
    logger.info("Initializing DynamoDB Service");
    logger.info("Batch size: {}, Flush interval: {}ms, Writer threads: {}",
        batchSize, flushIntervalMs, writerThreads);

    // Initialize DynamoDB client
    dynamoDbClient = DynamoDbClient.builder()
        .region(Region.of(awsRegion))
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build();

    // Start writer threads
    writerExecutor = Executors.newFixedThreadPool(writerThreads,
        r -> new Thread(r, "dynamodb-writer-" + System.currentTimeMillis()));

    // Start flush scheduler
    flushScheduler = Executors.newScheduledThreadPool(1,
        r -> new Thread(r, "dynamodb-flush-scheduler"));

    running = true;

    // Start writer threads
    for (int i = 0; i < writerThreads; i++) {
      writerExecutor.submit(new BatchWriter());
    }

    // Start periodic flush
    flushScheduler.scheduleAtFixedRate(
        this::flushBuffer,
        flushIntervalMs,
        flushIntervalMs,
        TimeUnit.MILLISECONDS
    );

    logger.info("DynamoDB Service initialized successfully");
  }

  @PreDestroy
  public void cleanup() {
    logger.info("Shutting down DynamoDB Service");
    running = false;

    // Flush remaining messages
    flushBuffer();

    // Shutdown executors
    if (flushScheduler != null) {
      flushScheduler.shutdown();
    }
    if (writerExecutor != null) {
      writerExecutor.shutdown();
      try {
        writerExecutor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        writerExecutor.shutdownNow();
      }
    }

    // Close client
    if (dynamoDbClient != null) {
      dynamoDbClient.close();
    }

    logger.info("DynamoDB Service stopped. Messages written: {}, Failed: {}",
        messagesWritten.get(), writesFailed.get());
  }

  /**
   * Queues a message for asynchronous database write (write-behind pattern)
   */
  public boolean queueMessageForWrite(TaskMessage message) {
    try {
      boolean added = writeBuffer.offer(message, 1, TimeUnit.SECONDS);
      if (!added) {
        logger.warn("Write buffer full, message dropped");
        writesFailed.incrementAndGet();
      }
      return added;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Interrupted while queuing message");
      return false;
    }
  }

  /**
   * Flush buffer immediately (called periodically and on shutdown)
   */
  private void flushBuffer() {
    List<TaskMessage> batch = new ArrayList<>(batchSize);
    writeBuffer.drainTo(batch, batchSize);

    if (!batch.isEmpty()) {
      logger.debug("Flushing {} messages from buffer", batch.size());
      writeBatchToDatabase(batch);
    }
  }

  /**
   * Background writer thread that continuously processes batched writes
   */
  private class BatchWriter implements Runnable {
    @Override
    public void run() {
      logger.info("Batch writer thread started");

      List<TaskMessage> batch = new ArrayList<>(batchSize);

      while (running || !writeBuffer.isEmpty()) {
        try {
          batch.clear();

          // Block on first message, then drain up to batch size
          TaskMessage first = writeBuffer.poll(1, TimeUnit.SECONDS);
          if (first != null) {
            batch.add(first);
            writeBuffer.drainTo(batch, batchSize - 1);
          }

          if (!batch.isEmpty()) {
            writeBatchToDatabase(batch);
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Writer thread interrupted");
          break;
        } catch (Exception e) {
          logger.error("Error in batch writer", e);
        }
      }

      logger.info("Batch writer thread stopped");
    }
  }

  /**
   * Writes a batch of messages to DynamoDB using BatchWriteItem
   */
  private void writeBatchToDatabase(List<TaskMessage> messages) {
    if (messages.isEmpty()) return;

    try {
      // Prepare batch write requests (max 25 items per request for DynamoDB)
      List<List<TaskMessage>> chunks = chunkList(messages, 25);

      for (List<TaskMessage> chunk : chunks) {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeRequests = new ArrayList<>();

        for (TaskMessage msg : chunk) {
          Map<String, AttributeValue> item = messageToAttributeMap(msg);

          PutRequest putRequest = PutRequest.builder()
              .item(item)
              .build();

          WriteRequest writeRequest = WriteRequest.builder()
              .putRequest(putRequest)
              .build();

          writeRequests.add(writeRequest);
        }

        requestItems.put(messagesTable, writeRequests);

        // Execute batch write with retry
        batchWriteWithRetry(requestItems);

        batchesWritten.incrementAndGet();
      }

      messagesWritten.addAndGet(messages.size());

      // Update room participation asynchronously
      updateRoomParticipation(messages);

      logger.debug("Wrote batch of {} messages to DynamoDB", messages.size());

    } catch (Exception e) {
      logger.error("Failed to write batch to DynamoDB", e);
      writesFailed.addAndGet(messages.size());
    }
  }

  /**
   * Execute batch write with exponential backoff retry
   */
  private void batchWriteWithRetry(Map<String, List<WriteRequest>> requestItems) {
    int maxRetries = 3;
    int attempt = 0;

    Map<String, List<WriteRequest>> unprocessed = requestItems;

    while (attempt < maxRetries && unprocessed != null && !unprocessed.isEmpty()) {
      try {
        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
            .requestItems(unprocessed)
            .build();

        BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(request);

        unprocessed = response.unprocessedItems();

        if (unprocessed != null && !unprocessed.isEmpty()) {
          attempt++;
          long backoff = (long) Math.pow(2, attempt) * 100;
          logger.warn("Unprocessed items: {}, retrying in {}ms",
              unprocessed.size(), backoff);
          Thread.sleep(backoff);
        }

      } catch (Exception e) {
        attempt++;
        if (attempt >= maxRetries) {
          logger.error("Failed to write batch after {} retries", maxRetries, e);
          throw new RuntimeException("Batch write failed", e);
        }

        long backoff = (long) Math.pow(2, attempt) * 100;
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted during retry", ie);
        }
      }
    }
  }

  /**
   * Convert QueueMessage to DynamoDB attribute map
   */
  private Map<String, AttributeValue> messageToAttributeMap(TaskMessage msg) {
    Map<String, AttributeValue> item = new HashMap<>();

    item.put("roomId", AttributeValue.builder().s(msg.getRoomId()).build());
    item.put("timestamp", AttributeValue.builder().s(msg.getTimestamp()).build());
    item.put("messageId", AttributeValue.builder().s(msg.getMessageId()).build());
    item.put("userId", AttributeValue.builder().s(msg.getUserId()).build());
    item.put("username", AttributeValue.builder().s(msg.getUsername()).build());
    item.put("message", AttributeValue.builder().s(msg.getMessage()).build());
    item.put("messageType", AttributeValue.builder().s(msg.getMessageType()).build());
    item.put("serverId", AttributeValue.builder().s(msg.getServerId()).build());
    item.put("clientIp", AttributeValue.builder().s(msg.getClientIp()).build());

    return item;
  }

  /**
   * Update room participation tracking
   */
  private void updateRoomParticipation(List<TaskMessage> messages) {
    // Group by user-room pairs
    Map<String, TaskMessage> userRoomMap = new HashMap<>();

    for (TaskMessage msg : messages) {
      String key = msg.getUserId() + "#" + msg.getRoomId();
      userRoomMap.put(key, msg);
    }

    // Update each user-room pair
    for (TaskMessage msg : userRoomMap.values()) {
      try {
        updateUserRoomParticipation(msg);
      } catch (Exception e) {
        logger.error("Failed to update room participation for user {} room {}",
            msg.getUserId(), msg.getRoomId(), e);
      }
    }
  }

  /**
   * Update or create room participation record for a user
   */
  private void updateUserRoomParticipation(TaskMessage msg) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put("userId", AttributeValue.builder().s(msg.getUserId()).build());
    key.put("roomId", AttributeValue.builder().s(msg.getRoomId()).build());

    Map<String, AttributeValueUpdate> updates = new HashMap<>();

    updates.put("username", AttributeValueUpdate.builder()
        .value(AttributeValue.builder().s(msg.getUsername()).build())
        .action(AttributeAction.PUT)
        .build());

    updates.put("lastActivityTime", AttributeValueUpdate.builder()
        .value(AttributeValue.builder().s(msg.getTimestamp()).build())
        .action(AttributeAction.PUT)
        .build());

    updates.put("messageCount", AttributeValueUpdate.builder()
        .value(AttributeValue.builder().n("1").build())
        .action(AttributeAction.ADD)
        .build());

    updates.put("lastMessageType", AttributeValueUpdate.builder()
        .value(AttributeValue.builder().s(msg.getMessageType()).build())
        .action(AttributeAction.PUT)
        .build());

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(participationTable)
        .key(key)
        .attributeUpdates(updates)
        .build();

    dynamoDbClient.updateItem(request);
  }

  /**
   * Chunk list into smaller lists
   */
  private <T> List<List<T>> chunkList(List<T> list, int chunkSize) {
    List<List<T>> chunks = new ArrayList<>();
    for (int i = 0; i < list.size(); i += chunkSize) {
      chunks.add(list.subList(i, Math.min(i + chunkSize, list.size())));
    }
    return chunks;
  }

  /**
   * Get current statistics
   */
  public String getStats() {
    return String.format("Written: %d, Failed: %d, Batches: %d, Buffer: %d",
        messagesWritten.get(), writesFailed.get(), batchesWritten.get(), writeBuffer.size());
  }

  /**
   * Get write buffer size
   */
  public int getBufferSize() {
    return writeBuffer.size();
  }
}