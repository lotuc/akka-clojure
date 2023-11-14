package org.lotuc;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import clojure.lang.IFn;
import clojure.lang.Var;
import clojure.lang.Symbol;
import clojure.lang.RT;

public class ClojureBehavior extends AbstractBehavior<ClojureBehavior.Message> {

  public static record Message (Object content, ActorRef<Message> replyTo) {}

  private static Symbol symNs = Symbol.intern("org.lotuc.akka-clojure");
  private static Symbol symContext = Symbol.intern("*context*");
  private static Symbol symReplyTo = Symbol.intern("*reply-to*");
  private static Var varContext = Var.intern(symNs, symContext);
  private static Var varReplyTo = Var.intern(symNs, symReplyTo);

  public static Behavior<Message> create(clojure.lang.IFn messageHandler) {
    return create(messageHandler, null);
  }

  public static Behavior<Message> create(
    clojure.lang.IFn messageHandler,
    clojure.lang.IFn setup) {
    return Behaviors.setup(ctx -> new ClojureBehavior(ctx, messageHandler, setup));
  }

  private final clojure.lang.IFn messageHandler;

  private ClojureBehavior(
    ActorContext<Message> context,
    clojure.lang.IFn messageHandler,
    clojure.lang.IFn setup) {
    super(context);
    this.messageHandler = messageHandler;
    if (setup != null) {
      Var.pushThreadBindings(RT.mapUniqueKeys(
                               varContext, getContext()));
      try {
        setup.invoke();
      } finally {
        Var.popThreadBindings();
      }
    }
  }

  @Override
  public Receive<ClojureBehavior.Message> createReceive() {
    return newReceiveBuilder().onMessage(Message.class, this::onMessage).build();
  }

  private Behavior<ClojureBehavior.Message> onMessage(ClojureBehavior.Message message) {
    Var.pushThreadBindings(RT.mapUniqueKeys(
                             varContext, getContext(),
                             varReplyTo, message.replyTo()));
    Behavior res = null;
    try {
      res = (Behavior) messageHandler.invoke(message.content());
    } finally {
      Var.popThreadBindings();
    }
    return res == null ? this : res;
  }
}
