package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

@RunWith(VertxUnitRunner.class)
public class ReceptionTest extends ArtemisTestBase {

  private Vertx vertx;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test(timeout = 20000)
  public void testReceiveBasicMessage(TestContext context) throws Exception {
    String testName = name.getMethodName();
    String sentContent = "myMessageContent-" + testName;
    String propKey = "appPropKey";
    String propValue = "appPropValue";

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    AmqpClient client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost(host).setPort(port).setPassword(password).setUsername(username));
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().receiver(testName, msg -> {
        context.assertNotNull(msg, "message was null");
        context.assertNotNull(msg.getBodyAsString(), "amqp message body content was null");
        context.assertEquals(sentContent, msg.getBodyAsString(), "amqp message body was not as expected");

        // Check the application property was present
        context.assertTrue(msg.getApplicationProperties() != null, "application properties element not present");
        JsonObject appProps = msg.getApplicationProperties();
        context.assertTrue(appProps.containsKey(propKey), "expected property key element not present");
        context.assertEquals(propValue, appProps.getValue(propKey), "app property value not as expected");
        client.close(x -> asyncShutdown.complete());
      }, done -> {
        context.assertEquals(testName, done.result().address(), "address was not as expected");

        ProtonClient proton = ProtonClient.create(vertx);
        proton.connect(host, port, username, password, res2 -> {
          context.assertTrue(res2.succeeded());
          org.apache.qpid.proton.message.Message protonMsg = Proton.message();
          protonMsg.setBody(new AmqpValue(sentContent));
          Map<String, Object> props = new HashMap<>();
          props.put(propKey, propValue);
          ApplicationProperties appProps = new ApplicationProperties(props);
          protonMsg.setApplicationProperties(appProps);
          ProtonConnection conn = res2.result().open();

          ProtonSender sender = conn.createSender(testName).open();
          sender.send(protonMsg, delivery -> {
            context.assertNotNull(delivery.getRemoteState(), "message had no remote state");
            context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message was not accepted");
            context.assertTrue(delivery.remotelySettled(), "message was not settled");
            conn.closeHandler(closeResult -> conn.disconnect()).close();
            asyncSendMsg.complete();
          });
        });
      });
    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReceiveBasicMessageAsStream(TestContext context) throws Exception {
    String testName = name.getMethodName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    AmqpClient client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost(host).setPort(port).setUsername(username).setPassword(password));
    client.connect(res -> {
      // Set up a read stream using the bridge
      res.result().receiver(testName, established -> {
        established.result().handler(msg -> {
          context.assertNotNull(msg, "message was null");

          String content = msg.getBodyAsString();
          context.assertNotNull(content, "amqp message body content was null");

          context.assertEquals(sentContent, content, "amqp message body was not as expected");

          client.close(shutdownRes -> {
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        });

        // Send it a message from a regular AMQP client
        ProtonClient proton = ProtonClient.create(vertx);
        proton.connect(host, port, username, password, res2 -> {
          context.assertTrue(res2.succeeded());

          org.apache.qpid.proton.message.Message protonMsg = Proton.message();
          protonMsg.setBody(new AmqpValue(sentContent));

          ProtonConnection conn = res2.result().open();

          ProtonSender sender = conn.createSender(testName).open();
          sender.send(protonMsg, delivery -> {
            context.assertNotNull(delivery.getRemoteState(), "message had no remote state");
            context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message was not accepted");
            context.assertTrue(delivery.remotelySettled(), "message was not settled");

            conn.closeHandler(closeResult -> conn.disconnect()).close();

            asyncSendMsg.complete();
          });

        });

      });

    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }
}
