package org.rembau.test.elasticsearch.tools;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Reader;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

public class GsonUtil {
	private static Gson gson = null;
	
	static{
		GsonBuilder gb = new GsonBuilder();
        gson = gb.create();
	}

	public static <T> T fromJson(String json, Class<T> classOfT) {
		return gson.fromJson(json, classOfT);
	}

    public static <T> T fromJson(String json, Class<T> classOfT, Type... classOfU) {
        Type objectType = type(classOfT, classOfU);
        return gson.fromJson(json, objectType);
    }

    public static <T> T fromJson(String json, Type type) {
        return gson.fromJson(json, type);
    }

	public static <T> T fromJson(Reader json, Class<T> classOfT) {
		return gson.fromJson(json, classOfT);
	}

    public static <T> T[] toArray(String json, Class<T> classOfT) {
        Class<?> aClass = Array.newInstance(classOfT, 0).getClass();
        return (T[]) gson.fromJson(json, aClass);
    }

    public static <T> List<T> toList(String json, Class<T> classOfT) {
        T[] array = toArray(json, classOfT);
        return Arrays.asList(array);
    }

	public static String toJson(Object src) {
		return gson.toJson(src);
	}

    public static String toJsonAndExclude(Object src, final String... fieldList) {
        if (fieldList == null || fieldList.length == 0) {
            return toJson(src);
        }

        Gson gson = new GsonBuilder().setExclusionStrategies(new ExclusionStrategy() {
            @Override
            public boolean shouldSkipField(FieldAttributes f) {
                for (String field : fieldList) {
                    if (field.equals(f.getName())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean shouldSkipClass(Class<?> clazz) {
                return false;
            }
        }).create();
        return gson.toJson(src);
    }


    private static ParameterizedType type(final Class raw, final Type... args) {
        return new ParameterizedType() {
            public Type getRawType() {
                return raw;
            }

            public Type[] getActualTypeArguments() {
                return args;
            }

            public Type getOwnerType() {
                return null;
            }
        };
    }
}
