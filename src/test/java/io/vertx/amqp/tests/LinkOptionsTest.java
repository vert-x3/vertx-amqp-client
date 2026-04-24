/*
 * Copyright (c) 2018-2019 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *        The Eclipse Public License is available at
 *        http://www.eclipse.org/legal/epl-v10.html
 *
 *        The Apache License v2.0 is available at
 *        http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.amqp.tests;

import io.vertx.amqp.AmqpClient;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.AmqpSenderOptions;
import io.vertx.amqp.SourceOptions;
import io.vertx.amqp.TargetOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonSession;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LinkOptionsTest extends BareTestBase {

  private MockServer server;

  @After
  @Override
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if (server != null) {
      server.close();
    }
  }

  @Test(timeout = 10000)
  public void testSenderLinkProperties(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    Map<String, Object> props = new HashMap<>();
    props.put("my-prop", "my-value");
    props.put("my-int", 42);

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.receiverOpenHandler(serverReceiver -> {
        Map<Symbol, Object> remoteProps = serverReceiver.getRemoteProperties();
        context.assertNotNull(remoteProps, "link properties should not be null");
        context.assertEquals("my-value", remoteProps.get(Symbol.valueOf("my-prop")));
        context.assertEquals(42, remoteProps.get(Symbol.valueOf("my-int")));
        context.assertEquals(2, remoteProps.size());

        serverReceiver.setTarget(((Target) serverReceiver.getRemoteTarget()).copy());
        serverReceiver.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      AmqpSenderOptions options = new AmqpSenderOptions().setLinkProperties(props);
      conn.createSender(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(sender -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testReceiverLinkProperties(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    Map<String, Object> props = new HashMap<>();
    props.put("recv-prop", "recv-value");

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.senderOpenHandler(serverSender -> {
        Map<Symbol, Object> remoteProps = serverSender.getRemoteProperties();
        context.assertNotNull(remoteProps, "link properties should not be null");
        context.assertEquals("recv-value", remoteProps.get(Symbol.valueOf("recv-prop")));
        context.assertEquals(1, remoteProps.size());

        serverSender.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      AmqpReceiverOptions options = new AmqpReceiverOptions().setLinkProperties(props);
      conn.createReceiver(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(receiver -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testSenderTargetTerminusOptions(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = (Target) serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget);
        context.assertEquals(TerminusDurability.CONFIGURATION, remoteTarget.getDurable());
        context.assertEquals(TerminusExpiryPolicy.SESSION_END, remoteTarget.getExpiryPolicy());
        context.assertEquals(UnsignedInteger.valueOf(60), remoteTarget.getTimeout());

        Symbol[] caps = remoteTarget.getCapabilities();
        context.assertNotNull(caps);
        Symbol[] expected = new Symbol[]{Symbol.valueOf("topic")};
        context.assertTrue(Arrays.equals(expected, caps), "Unexpected capabilities: " + Arrays.toString(caps));

        serverReceiver.setTarget(remoteTarget.copy());
        serverReceiver.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      TargetOptions targetOpts = new TargetOptions()
        .setDurability("CONFIGURATION")
        .setExpiryPolicy("SESSION_END")
        .setTimeout(60)
        .setCapabilities(List.of("topic"));
      AmqpSenderOptions options = new AmqpSenderOptions().setTargetOptions(targetOpts);
      conn.createSender(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(sender -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testSenderSourceTerminusOptions(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.receiverOpenHandler(serverReceiver -> {
        Source remoteSource = (Source) serverReceiver.getRemoteSource();
        context.assertNotNull(remoteSource);
        context.assertEquals(TerminusDurability.NONE, remoteSource.getDurable());
        context.assertEquals(TerminusExpiryPolicy.LINK_DETACH, remoteSource.getExpiryPolicy());

        serverReceiver.setTarget(((Target) serverReceiver.getRemoteTarget()).copy());
        serverReceiver.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      SourceOptions sourceOpts = new SourceOptions()
        .setDurability("NONE")
        .setExpiryPolicy("LINK_DETACH");
      AmqpSenderOptions options = new AmqpSenderOptions().setSourceOptions(sourceOpts);
      conn.createSender(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(sender -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testReceiverSourceTerminusOptions(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource);
        context.assertEquals(TerminusDurability.UNSETTLED_STATE, remoteSource.getDurable());
        context.assertEquals(TerminusExpiryPolicy.NEVER, remoteSource.getExpiryPolicy());
        context.assertEquals(UnsignedInteger.valueOf(120), remoteSource.getTimeout());

        Symbol[] caps = remoteSource.getCapabilities();
        context.assertNotNull(caps);
        Symbol[] expected = new Symbol[]{Symbol.valueOf("shared")};
        context.assertTrue(Arrays.equals(expected, caps), "Unexpected capabilities: " + Arrays.toString(caps));

        serverSender.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      SourceOptions sourceOpts = new SourceOptions()
        .setDurability("UNSETTLED_STATE")
        .setExpiryPolicy("NEVER")
        .setTimeout(120)
        .setCapabilities(List.of("shared"));
      AmqpReceiverOptions options = new AmqpReceiverOptions().setSourceOptions(sourceOpts);
      conn.createReceiver(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(receiver -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testReceiverTargetTerminusOptions(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.senderOpenHandler(serverSender -> {
        Target remoteTarget = (Target) serverSender.getRemoteTarget();
        context.assertNotNull(remoteTarget);
        context.assertEquals(TerminusDurability.CONFIGURATION, remoteTarget.getDurable());
        context.assertEquals(TerminusExpiryPolicy.CONNECTION_CLOSE, remoteTarget.getExpiryPolicy());

        serverSender.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      TargetOptions targetOpts = new TargetOptions()
        .setDurability("CONFIGURATION")
        .setExpiryPolicy("CONNECTION_CLOSE");
      AmqpReceiverOptions options = new AmqpReceiverOptions().setTargetOptions(targetOpts);
      conn.createReceiver(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(receiver -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testReceiverSourceOptionsOverrideDurable(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource);
        // SourceOptions should override the durable=true convenience (which sets UNSETTLED_STATE + NEVER)
        context.assertEquals(TerminusDurability.CONFIGURATION, remoteSource.getDurable());
        context.assertEquals(TerminusExpiryPolicy.SESSION_END, remoteSource.getExpiryPolicy());

        serverSender.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      SourceOptions sourceOpts = new SourceOptions()
        .setDurability("CONFIGURATION")
        .setExpiryPolicy("SESSION_END");
      AmqpReceiverOptions options = new AmqpReceiverOptions()
        .setDurable(true)
        .setSourceOptions(sourceOpts);
      conn.createReceiver(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(receiver -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testSenderTargetOptionsCapabilitiesOverride(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = (Target) serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget);

        // TargetOptions capabilities should override AmqpSenderOptions capabilities
        Symbol[] caps = remoteTarget.getCapabilities();
        context.assertNotNull(caps);
        Symbol[] expected = new Symbol[]{Symbol.valueOf("from-target-options")};
        context.assertTrue(Arrays.equals(expected, caps),
          "TargetOptions capabilities should override, got: " + Arrays.toString(caps));

        serverReceiver.setTarget(remoteTarget.copy());
        serverReceiver.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      TargetOptions targetOpts = new TargetOptions().setCapabilities(List.of("from-target-options"));
      AmqpSenderOptions options = new AmqpSenderOptions()
        .addCapability("from-sender-options")
        .setTargetOptions(targetOpts);
      conn.createSender(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(sender -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testCaseInsensitiveDurabilityAndExpiryPolicy(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = (Target) serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget);
        context.assertEquals(TerminusDurability.CONFIGURATION, remoteTarget.getDurable());
        context.assertEquals(TerminusExpiryPolicy.NEVER, remoteTarget.getExpiryPolicy());

        serverReceiver.setTarget(remoteTarget.copy());
        serverReceiver.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      TargetOptions targetOpts = new TargetOptions()
        .setDurability("configuration")
        .setExpiryPolicy("never");
      AmqpSenderOptions options = new AmqpSenderOptions().setTargetOptions(targetOpts);
      conn.createSender(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(sender -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testSenderSourceAddress(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.receiverOpenHandler(serverReceiver -> {
        Source remoteSource = (Source) serverReceiver.getRemoteSource();
        context.assertNotNull(remoteSource);
        context.assertEquals("my-source-address", remoteSource.getAddress());

        serverReceiver.setTarget(((Target) serverReceiver.getRemoteTarget()).copy());
        serverReceiver.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      SourceOptions sourceOpts = new SourceOptions().setAddress("my-source-address");
      AmqpSenderOptions options = new AmqpSenderOptions().setSourceOptions(sourceOpts);
      conn.createSender(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(sender -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testSenderTargetAddress(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = (Target) serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget);
        context.assertEquals("my-target-address", remoteTarget.getAddress());

        serverReceiver.setTarget(remoteTarget.copy());
        serverReceiver.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      TargetOptions targetOpts = new TargetOptions().setAddress("my-target-address");
      AmqpSenderOptions options = new AmqpSenderOptions().setTargetOptions(targetOpts);
      conn.createSender(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(sender -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testReceiverSourceAddress(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource);
        context.assertEquals("my-source-address", remoteSource.getAddress());

        serverSender.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      SourceOptions sourceOpts = new SourceOptions().setAddress("my-source-address");
      AmqpReceiverOptions options = new AmqpReceiverOptions().setSourceOptions(sourceOpts);
      conn.createReceiver(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(receiver -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testReceiverTargetAddress(TestContext context) throws Exception {
    Async serverAsync = context.async();
    Async clientAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());

      serverConnection.senderOpenHandler(serverSender -> {
        Target remoteTarget = (Target) serverSender.getRemoteTarget();
        context.assertNotNull(remoteTarget);
        context.assertEquals("my-target-address", remoteTarget.getAddress());

        serverSender.open();
        serverAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      TargetOptions targetOpts = new TargetOptions().setAddress("my-target-address");
      AmqpReceiverOptions options = new AmqpReceiverOptions().setTargetOptions(targetOpts);
      conn.createReceiver(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(receiver -> {
        clientAsync.complete();
      }));
    }));

    serverAsync.awaitSuccess();
    clientAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testSourceOptionsJsonRoundTrip(TestContext context) {
    SourceOptions original = new SourceOptions()
      .setAddress("source-addr")
      .setDurability("UNSETTLED_STATE")
      .setExpiryPolicy("NEVER")
      .setTimeout(300)
      .setCapabilities(List.of("shared", "global"));

    SourceOptions fromJson = new SourceOptions(original.toJson());
    context.assertEquals(original.getAddress(), fromJson.getAddress());
    context.assertEquals(original.getDurability(), fromJson.getDurability());
    context.assertEquals(original.getExpiryPolicy(), fromJson.getExpiryPolicy());
    context.assertEquals(original.getTimeout(), fromJson.getTimeout());
    context.assertEquals(original.getCapabilities(), fromJson.getCapabilities());
  }

  @Test(timeout = 10000)
  public void testTargetOptionsJsonRoundTrip(TestContext context) {
    TargetOptions original = new TargetOptions()
      .setAddress("target-addr")
      .setDurability("CONFIGURATION")
      .setExpiryPolicy("SESSION_END")
      .setTimeout(60)
      .setCapabilities(List.of("topic"));

    TargetOptions fromJson = new TargetOptions(original.toJson());
    context.assertEquals(original.getAddress(), fromJson.getAddress());
    context.assertEquals(original.getDurability(), fromJson.getDurability());
    context.assertEquals(original.getExpiryPolicy(), fromJson.getExpiryPolicy());
    context.assertEquals(original.getTimeout(), fromJson.getTimeout());
    context.assertEquals(original.getCapabilities(), fromJson.getCapabilities());
  }
}
