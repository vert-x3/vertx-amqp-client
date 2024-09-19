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
package io.vertx.amqp.tests.impl;

import static org.junit.Assert.*;

import io.vertx.amqp.impl.AmqpMessageImpl;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;

public class AmqpMessageImplTest {

  @Test
  public void testContentEncoding() {
    String contentEncoding = "some-encoding";

    Message protonMsg = Message.Factory.create();
    protonMsg.setContentEncoding(contentEncoding);

    AmqpMessageImpl message = new AmqpMessageImpl(protonMsg);
    assertEquals(contentEncoding, message.contentEncoding());
  }

  @Test
  public void testIsBodyNull() {

    Message protonMsg = Message.Factory.create();
    AmqpMessageImpl message = new AmqpMessageImpl(protonMsg);

    protonMsg.setBody(null);
    assertTrue(message.isBodyNull());

    protonMsg.setBody(new AmqpValue(null));
    assertTrue(message.isBodyNull());

    protonMsg.setBody(new Data(new Binary(new byte[0])));
    assertFalse(message.isBodyNull());
  }

}
