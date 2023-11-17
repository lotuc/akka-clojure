package lotuc.akka.serialization;

import akka.actor.ExtendedActorSystem;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.ActorRefResolver;
import akka.actor.typed.javadsl.Adapter;
import akka.serialization.JSerializer;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;

// https://doc.akka.io/docs/akka/current/serialization.html#serializing-actorrefs
public class NippySerializer extends JSerializer {
  private static IFn require = RT.var("clojure.core", "require").fn();
  private static IFn freeze;
  private static IFn thaw;
  private static IFn get;

  static {
    require.invoke(RT.readString("taoensso.nippy"));
    freeze = RT.var("taoensso.nippy", "freeze");
    thaw = RT.var("taoensso.nippy", "thaw");
    get = RT.var("clojure.core", "get");
  }

  private final IPersistentMap bindings;

  public NippySerializer(ExtendedActorSystem extendedActorSystem) {
    ActorSystem actorSystem = Adapter.toTyped(extendedActorSystem);
    ActorRefResolver actorRefResolver = ActorRefResolver.get(actorSystem);

    require.invoke(RT.readString("lotuc.akka.serialization.nippy"));
    Var varExtendedActorSystem;
    Var varActorSystem;
    Var varActorRefResolver;

    varExtendedActorSystem =
      RT.var("lotuc.akka.serialization.nippy", "*extended-actor-system*");
    varActorSystem =
      RT.var("lotuc.akka.serialization.nippy", "*actor-system*");
    varActorRefResolver =
      RT.var("lotuc.akka.serialization.nippy", "*actor-ref-resolver*");

    this.bindings = PersistentHashMap.EMPTY
      .assoc(varActorSystem, actorSystem)
      .assoc(varExtendedActorSystem, extendedActorSystem)
      .assoc(varActorRefResolver, actorRefResolver);
  }

  @Override
  public Object fromBinaryJava(byte[] bytes, Class<?> manifest) {
    Var.pushThreadBindings(bindings);
    try {
      return thaw.invoke(bytes);
    } finally {
      Var.popThreadBindings();
    }
  }

  @Override
  public int identifier() {
    return 246990042;
  }

  @Override
  public byte[] toBinary(Object o) {
    Var.pushThreadBindings(bindings);
    try {
      return (byte[])freeze.invoke(o);
    } finally {
      Var.popThreadBindings();
    }
  }

  @Override
  public boolean includeManifest() {
    return false;
  }
}
