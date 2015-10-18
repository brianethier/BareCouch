package ca.barelabs.barecouch;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DocumentUtils {

	
	public static String getId(Object document) {
		return callGetMethod(document, "getId");
	}
	
	public static void setId(Object document, String id) {
		if(!callSetMethod(document, "setId", id)) {
			setFieldValue(document, "id", id);
		}
	}
	
	public static String getRev(Object document) {
		return callGetMethod(document, "getRev");
	}
	
	public static void setRev(Object document, String rev) {
		if(!callSetMethod(document, "setRev", rev)) {
			setFieldValue(document, "rev", rev);
		}
	}
	
	private static String callGetMethod(Object document, String methodName) {
		try {
			Method method = document.getClass().getMethod(methodName);
			if(method != null) {
				Object value = method.invoke(document);
				if(value instanceof String) {
					return (String) value;
				}
			}
		} catch (NoSuchMethodException e) {
		} catch (SecurityException e) {
		} catch (IllegalAccessException e) {
		} catch (IllegalArgumentException e) {
		} catch (InvocationTargetException e) {
		}
		return null;
	}
	
	private static boolean callSetMethod(Object document, String methodName, String value) {
		try {
			Method method = document.getClass().getMethod(methodName, String.class);
			if(method != null) {
				method.invoke(document, value);
				return true;
			}
		} catch (NoSuchMethodException e) {
		} catch (SecurityException e) {
		} catch (IllegalAccessException e) {
		} catch (IllegalArgumentException e) {
		} catch (InvocationTargetException e) {
		}
		return false;
	}
	
	private static boolean setFieldValue(Object document, String fieldName, String value) {
		try {
			Field field = document.getClass().getDeclaredField(fieldName);
			if(field != null && String.class.isAssignableFrom(field.getType())) {
				field.set(document, value);
				return true;
			}
		} catch (NoSuchFieldException e) {
		} catch (SecurityException e) {
		} catch (IllegalArgumentException e) {
		} catch (IllegalAccessException e) {
		}
		return false;
	}
}
