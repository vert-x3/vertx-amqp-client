package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.CountDownLatch;

public class ArtemisTestBase {

  @ClassRule
  public static GenericContainer artemis = new GenericContainer("vromero/activemq-artemis:2.6.3-alpine")
    .withExposedPorts(8161)
    .withExposedPorts(5672);

  private Vertx vertx;
  String host;
  int port;
  String username;
  String password;
  AmqpUsage usage;


  @Before
  public void setup() {
    vertx = Vertx.vertx();
    host = artemis.getContainerIpAddress();
    port = artemis.getMappedPort(5672);
    username = "artemis";
    password = "simetraehcapa";
    System.setProperty("amqp-host", host);
    System.setProperty("amqp-port", Integer.toString(port));
    System.setProperty("amqp-user", "artemis");
    System.setProperty("amqp-pwd", "simetraehcapa");
    usage = new AmqpUsage(vertx, host, port);
  }

  @After
  public void tearDown() throws InterruptedException {
    System.clearProperty("amqp-host");
    System.clearProperty("amqp-port");
    System.clearProperty("amqp-user");
    System.clearProperty("amqp-pwd");

    CountDownLatch latch = new CountDownLatch(1);
    usage.close();
    vertx.close(x -> latch.countDown());

    latch.await();
  }
}
