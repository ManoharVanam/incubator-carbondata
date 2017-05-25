package org.apache.carbondata.core.util;

/**
 * It stores carbon session params
 */
public class ThreadLocalSessionParams {
   final static ThreadLocal<SessionParams> threadLocal = new ThreadLocal<SessionParams>();

public static void setSessionParams(SessionParams sessionParams) {
  threadLocal.set(sessionParams);
}

  public static SessionParams getSessionParams() {
    return threadLocal.get();
  }
}
