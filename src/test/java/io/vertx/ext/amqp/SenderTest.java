package io.vertx.ext.amqp;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SenderTest extends ArtemisTestBase {

  //TODO Test the error with bad credentials

  @Test
  public void testThatMessagedAreSent() {
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    usage.consumeStrings(queue, 1, 1, TimeUnit.MINUTES, null, list::add);
    AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).connect(connection -> {
        connection.result().sender(queue, done -> {
          if (done.failed()) {
            System.out.println("Unable to get a sender: " + done.cause());
            done.cause().printStackTrace();
          } else {
            // Sending a few messages
            done.result().send(AmqpMessage.create().body("hello").address(queue).build());
            done.result().send(AmqpMessage.create().body("world").address(queue).build());
          }
        });
      }
    );

    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly("hello", "world");
  }

  @Test
  public void testThatMessagedAreAcknowledged() {
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    AtomicInteger acks = new AtomicInteger();
    usage.consumeStrings(queue, 1, 1, TimeUnit.MINUTES, null, list::add);
    AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).connect(connection -> {
        connection.result().sender(queue, done -> {
          if (done.failed()) {
            done.cause().printStackTrace();
          } else {
            // Sending a few messages
            done.result().sendWithAck(AmqpMessage.create().body("hello").address(queue).build(), x -> {
              if (x.succeeded()) {
                acks.incrementAndGet();
              }
            });
            done.result().sendWithAck(AmqpMessage.create().body("world").address(queue).build(), x -> {
              if (x.succeeded()) {
                acks.incrementAndGet();
              }
            });
          }
        });
      }
    );

    await().until(() -> list.size() == 2);
    await().until(() -> acks.get() == 2);
    assertThat(list).containsExactly("hello", "world");
  }
}
