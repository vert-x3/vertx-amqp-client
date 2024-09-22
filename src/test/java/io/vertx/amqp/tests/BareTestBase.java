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
import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
      client.close().onComplete(x -> latchForClient.countDown());
      latchForClient.await(10, TimeUnit.SECONDS);
    }
    vertx.close().onComplete(x -> latchForVertx.countDown());
    latchForVertx.await(10, TimeUnit.SECONDS);
  }

  @Test
  public void justToAvoidTheIdeToFail() {

  }
}
