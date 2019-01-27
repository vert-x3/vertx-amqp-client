package io.vertx.ext.amqp;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

public class ConnectionTest extends ArtemisTestBase {

  @Test
  public void testConnectionSuccessWithDetailsPassedInOptions() {
    AtomicBoolean done = new AtomicBoolean();
    AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
    ).connect(
      ar -> done.set(ar.succeeded())
    );

    await().untilAtomic(done, is(true));
  }

  @Test
  public void testConnectionSuccessWithDetailsPassedAsSystemVariables() {
    System.setProperty("amqp-client-host", host);
    System.setProperty("amqp-client-port", Integer.toString(port));
    AtomicBoolean done = new AtomicBoolean();
    AmqpClient.create().connect(
      ar -> done.set(ar.succeeded())
    );

    await().untilAtomic(done, is(true));

    System.clearProperty("amqp-client-host");
    System.clearProperty("amqp-client-port");
  }

  @Test
  public void testConnectionFailedBecauseOfBadHost() {
    AtomicBoolean done = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AmqpClient.create(new AmqpClientOptions()
      .setHost("org.acme")
      .setPort(port)
    ).connect(
      ar -> {
        failure.set(ar.cause());
        done.set(true);
      }
    );

    await().pollInterval(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS)
      .untilAtomic(done, is(true));
    assertThat(failure.get()).isNotNull();
  }
}
