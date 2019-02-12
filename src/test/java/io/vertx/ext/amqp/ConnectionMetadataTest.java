package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(VertxUnitRunner.class)
public class ConnectionMetadataTest {

  private Vertx vertx;
  private MockServer server;

  @Before
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test
  public void testMetadata(TestContext context) throws ExecutionException, InterruptedException {
    Async asyncMetaData = context.async();
    Async asyncShutdown = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.openHandler(x -> {
        // Open the connection.
        serverConnection.open();

        // Validate the properties separately.
        Map<Symbol, Object> properties = serverConnection.getRemoteProperties();

        context.assertNotNull(properties, "connection properties not present");

        context.assertTrue(properties.containsKey(AmqpConnection.PRODUCT_KEY),
          "product property key not present");
        context.assertEquals(AmqpConnection.PRODUCT, properties.get(AmqpConnection.PRODUCT_KEY),
          "unexpected product property value");

        asyncMetaData.complete();
      });
    });

    AmqpClient.create(new AmqpClientOptions().setHost("localhost").setPort(server.actualPort())).connect(ar -> {
      if (ar.failed()) {
        context.fail(ar.cause());
      } else {
        ar.result().close(x -> {
          if (x.failed()) {
            context.fail(x.cause());
          } else {
            asyncShutdown.complete();
          }
        });
      }
    });
  }

  @Test(timeout = 20000)
  public void testConnectionHostnameAndContainerID(TestContext context) throws Exception {
    doConnectionHostnameAndContainerIDTestImpl(context, true);
    doConnectionHostnameAndContainerIDTestImpl(context, false);
  }

  private void doConnectionHostnameAndContainerIDTestImpl(TestContext context, boolean customValues) throws Exception {
    String tcpConnectionHostname = "localhost";
    String containerId = "myCustomContainer";
    String vhost = "myCustomVhost";

    Async asyncShutdown = context.async();
    AtomicBoolean linkOpened = new AtomicBoolean();

    MockServer server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(x -> {
        if(customValues){
          context.assertEquals(vhost, serverConnection.getRemoteHostname());
          context.assertFalse(tcpConnectionHostname.equals(serverConnection.getRemoteHostname()));

          context.assertEquals(containerId, serverConnection.getRemoteContainer());
        } else{
          context.assertEquals(tcpConnectionHostname, serverConnection.getRemoteHostname());
          context.assertNotNull(containerId, serverConnection.getRemoteContainer());
        }
        serverConnection.open();
      });
      serverConnection.closeHandler(x -> {
        serverConnection.close();
      });
    });

    AmqpClientOptions opts = new AmqpClientOptions()
      .setHost(tcpConnectionHostname).setPort(server.actualPort());
    if (customValues) {
      opts.setContainerId(containerId).setVirtualHost(vhost);
    }


    AmqpClient client = AmqpClient.create(opts)
      .connect(res -> {
      context.assertTrue(res.succeeded(), "Expected connection to succeed");

      res.result().close(shutdownRes -> {
        context.assertTrue(shutdownRes.succeeded());
        asyncShutdown.complete();
      });
    });

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }

    context.assertFalse(linkOpened.get());
  }

}
