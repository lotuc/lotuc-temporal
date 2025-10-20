package lotuc.sci_rt.temporal;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.IExceptionInfo;
import clojure.lang.PersistentArrayMap;
import clojure.java.api.Clojure;

public class DoNotRetryExceptionInfo extends RuntimeException implements IExceptionInfo {

  private static final long serialVersionUID = -1073474305916521986L;
  private static final Keyword k_retryable = (Keyword) ((IFn) Clojure.var("clojure.core", "keyword")).invoke("temporal", "retryable");
  private static final IFn assoc = Clojure.var("clojure.core", "assoc");

  public final IPersistentMap data;

  public DoNotRetryExceptionInfo(String s, IPersistentMap data) {
    this(s, data, null);
  }

  public DoNotRetryExceptionInfo(String s, IPersistentMap data, Throwable throwable) {
    // null cause is equivalent to not passing a cause
    super(s, throwable);
    IPersistentMap data0 = (data == null) ? PersistentArrayMap.EMPTY: data;
    this.data = (IPersistentMap) assoc.invoke(data0, k_retryable, false);
  }

  public IPersistentMap getData() {
    return data;
  }

  public String toString() {
    return "lotuc.temporal.DoNotRetryExceptionInfo: " + getMessage() + " " + data.toString();
  }
}
