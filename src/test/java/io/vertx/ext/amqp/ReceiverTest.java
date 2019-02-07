package io.vertx.ext.amqp;

import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ReceiverTest extends ArtemisTestBase {

  @Test
  public void testReception() {
    AtomicInteger count = new AtomicInteger();
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).connect(connection -> {
        connection.result().receiver(queue, message -> list.add(message.getBodyAsString()),
          done ->
            CompletableFuture.runAsync(() -> {
              usage.produceStrings(queue, 10, null,
                () -> Integer.toString(count.getAndIncrement()));
            })
        );
      }
    );

    await().until(() -> list.size() == 10);
    assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
  }
}
