package io.dropwizard.discovery.common;

import com.google.common.base.Strings;

import java.util.Collection;
import java.util.Map;

public interface CommonUtils {

    static boolean isNullOrEmpty(String s) {
        return Strings.isNullOrEmpty(s);
    }

    static <T> boolean isNullOrEmpty(Collection<T> collection) {
        return null == collection
                || collection.isEmpty();
    }

    static <T> boolean isNullOrEmpty(Map map) {
        return null == map
                || map.isEmpty();
    }

}
