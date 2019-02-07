package io.vertx.ext.amqp;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.vertx.proton.ProtonHelper.message;

public class AmqpUsage {

  private static Logger LOGGER = LoggerFactory.getLogger(AmqpUsage.class);
  private final Context context;
  private ProtonClient client;
  private ProtonConnection connection;

  private List<ProtonSender> senders = new CopyOnWriteArrayList<>();
  private List<ProtonReceiver> receivers = new CopyOnWriteArrayList<>();


  public AmqpUsage(Vertx vertx, String host, int port) {
    this(vertx, host, port, "artemis", "simetraehcapa");
  }

  public AmqpUsage(Vertx vertx, String host, int port, String user, String pwd) {
    CountDownLatch latch = new CountDownLatch(1);
    this.context = vertx.getOrCreateContext();
    context.runOnContext(x -> {
      client = ProtonClient.create(vertx);
      client.connect(host, port, user, pwd, conn -> {
        if (conn.succeeded()) {
          LOGGER.info("Connection to the AMQP host succeeded");
          this.connection = conn.result();
          this.connection
            .openHandler(connection -> latch.countDown())
            .open();
        }
      });
    });
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Use the supplied function to asynchronously produce messages and write them to the host.
   *
   * @param topic              the topic, must not be null
   * @param messageCount       the number of messages to produce; must be positive
   * @param completionCallback the function to be called when the producer is completed; may be null
   * @param messageSupplier    the function to produce messages; may not be null
   */
  public void produce(String topic, int messageCount, Runnable completionCallback, Supplier<Object> messageSupplier) {
    CountDownLatch ready = new CountDownLatch(1);
    ProtonSender sender = connection.createSender(topic);
    senders.add(sender);
    context.runOnContext(x -> {
      sender
        .openHandler(s -> ready.countDown())
        .open();
    });

    try {
      ready.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Thread t = new Thread(() -> {
      LOGGER.info("Starting AMQP sender to write {} messages", messageCount);
      try {
        for (int i = 0; i != messageCount; ++i) {
          Object payload = messageSupplier.get();
          Message message = message();
          if (payload instanceof Section) {
            message.setBody((Section) payload);
          } else if (payload != null) {
            message.setBody(new AmqpValue(payload));
          } else {
            // Don't set a body.
          }
          message.setDurable(true);
          message.setTtl(10000);
          CountDownLatch latch = new CountDownLatch(1);
          context.runOnContext((y) ->
            sender.send(message, x ->
              latch.countDown()
            )
          );
          latch.await();
          LOGGER.info("Producer sent message {}", payload);
        }
      } catch (Exception e) {
        LOGGER.error("Unable to send message", e);
      } finally {
        if (completionCallback != null) {
          completionCallback.run();
        }
        context.runOnContext(x -> sender.close());
      }
    });
    t.setName(topic + "-thread");
    t.start();

    try {
      ready.await();
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while waiting for the ProtonSender to be opened", e);
    }
  }

  public void produceStrings(String topic, int messageCount, Runnable completionCallback, Supplier<String> messageSupplier) {
    this.produce(topic, messageCount, completionCallback, messageSupplier::get);
  }

  public void produceIntegers(String topic, int messageCount, Runnable completionCallback, Supplier<Integer> messageSupplier) {
    this.produce(topic, messageCount, completionCallback, messageSupplier::get);
  }

  /**
   * Use the supplied function to asynchronously consume messages from the cluster.
   *
   * @param topic            the topic
   * @param continuation     the function that determines if the consumer should continue; may not be null
   * @param completion       the function to call when the consumer terminates; may be null
   * @param consumerFunction the function to consume the messages; may not be null
   */
  public void consume(String topic, BooleanSupplier continuation, Runnable completion,
                      Consumer<AmqpMessage> consumerFunction) {
    CountDownLatch latch = new CountDownLatch(1);
    ProtonReceiver receiver = connection.createReceiver(topic);
    receivers.add(receiver);
    Thread t = new Thread(() -> {
      try {
        context.runOnContext(x -> {
          receiver.handler((delivery, message) -> {
            LOGGER.info("Consumer {}: consuming message {}", topic, message.getBody());
            consumerFunction.accept(AmqpMessage.create(message).build());
            if (!continuation.getAsBoolean()) {
              receiver.close();
            }
          })
            .openHandler(r -> {
              LOGGER.info("Starting consumer to read messages on {}", topic);
              latch.countDown();
            })
            .open();
        });
      } catch (Exception e) {
        LOGGER.error("Unable to receive messages from {}", topic, e);
      } finally {
        if (completion != null) {
          completion.run();
        }
      }
    });
    t.setName(topic + "-thread");
    t.start();
    try {
      latch.await();
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while waiting for the ProtonReceiver to be opened", e);
    }
  }

  public void consumeStrings(String topic, BooleanSupplier continuation, Runnable completion, Consumer<String> consumerFunction) {
    this.consume(topic, continuation, completion, value -> consumerFunction.accept(value.getBodyAsString()));
  }

  public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completion, Consumer<String> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.consumeStrings(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), completion, s -> {
      consumer.accept(s);
      readCounter.incrementAndGet();
    });
  }

  public void consumeMessages(String topicName, int count, long timeout, TimeUnit unit, Runnable completion, Consumer<AmqpMessage> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.consume(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), completion, s -> {
      consumer.accept(s);
      readCounter.incrementAndGet();
    });
  }

  private BooleanSupplier continueIfNotExpired(BooleanSupplier continuation,
                                               long timeout, TimeUnit unit) {
    return new BooleanSupplier() {
      long stopTime = 0L;

      public boolean getAsBoolean() {
        if (this.stopTime == 0L) {
          this.stopTime = System.currentTimeMillis() + unit.toMillis(timeout);
        }

        return continuation.getAsBoolean() && System.currentTimeMillis() <= this.stopTime;
      }
    };
  }

  public void close() throws InterruptedException {
    CountDownLatch entities = new CountDownLatch(senders.size() + receivers.size());
    senders.forEach(sender -> {
      if (sender.isOpen()) {
        sender.closeHandler(x -> entities.countDown()).close();
      } else {
        entities.countDown();
      }
    });
    receivers.forEach(receiver -> {
      if (receiver.isOpen()) {
        receiver.closeHandler(x -> entities.countDown()).close();
      } else {
        entities.countDown();
      }
    });
    entities.await(30, TimeUnit.SECONDS);


    if (connection != null && !connection.isDisconnected()) {
      CountDownLatch latch = new CountDownLatch(1);
      context.runOnContext(n ->
        connection
          .closeHandler(x -> latch.countDown())
          .close());
      latch.await(10, TimeUnit.SECONDS);
    }


  }
}

