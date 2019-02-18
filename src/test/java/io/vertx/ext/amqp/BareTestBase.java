package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(VertxUnitRunner.class)
public class BareTestBase {

  @Rule
  public TestName name = new TestName();

  protected AmqpClient client;

  protected Vertx vertx;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() throws InterruptedException {
    CountDownLatch latchForClient = new CountDownLatch(1);
    CountDownLatch latchForVertx = new CountDownLatch(1);
    if (client != null) {
      client.close(x -> latchForClient.countDown());
      latchForClient.await(10, TimeUnit.SECONDS);
    }
    vertx.close(x -> latchForVertx.countDown());
    latchForVertx.await(10, TimeUnit.SECONDS);
  }
}
