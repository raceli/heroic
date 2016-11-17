package com.spotify.heroic;

import java.lang.instrument.Instrumentation;
import javax.naming.ServiceUnavailableException;

public class HeroicInstrumentation {
    private static Instrumentation instrumentation = null;

    public static void premain(String args, Instrumentation i) {
        instrumentation = i;
        System.out.println("premain done");
    }

    public static long getObjectSize(Object obj) throws ServiceUnavailableException {
        if (instrumentation == null) {
            throw new ServiceUnavailableException();
        }
        return instrumentation.getObjectSize(obj);
    }
}

