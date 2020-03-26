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
package io.vertx.amqp;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.RepeatRule;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(VertxUnitRunner.class)
public class DisconnectTest extends ArtemisTestBase {

  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test
  public void testUseConnectionAfterDisconnect(TestContext ctx) {
    String queue = UUID.randomUUID().toString();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    );
    client.connect(ctx.asyncAssertSuccess(conn -> {
      conn.exceptionHandler(err -> {
        conn.createSender(queue, ctx.asyncAssertFailure(sender -> {
        }));
        conn.createAnonymousSender(ctx.asyncAssertFailure(sender -> {
        }));
        conn.createReceiver("some-address", ctx.asyncAssertFailure(sender -> {
        }));
        conn.createReceiver("some-address", new AmqpReceiverOptions(), ctx.asyncAssertFailure(sender -> {
        }));
        conn.createDynamicReceiver(ctx.asyncAssertFailure(sender -> {
        }));
      });
      vertx.executeBlocking(promise -> {
        artemis.close();
        promise.complete();
      }, ctx.asyncAssertSuccess(v -> {
      }));
    }));
  }
}
