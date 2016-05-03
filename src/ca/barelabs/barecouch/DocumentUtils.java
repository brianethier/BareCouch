package ca.barelabs.barecouch;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DocumentUtils {

	public static final String ID_FIELD = "id";
	public static final String ID_GET_METHOD = "getId";
	public static final String ID_SET_METHOD = "setId";
	public static final String REV_FIELD = "rev";
	public static final String REV_GET_METHOD = "getRev";
	public static final String REV_SET_METHOD = "setRev";
	
	public static String getId(Object document) {
		if (isDocumentValid(document)) {
			String docId = callGetMethod(document, ID_GET_METHOD);
			return docId == null ? getFieldValue(document, ID_FIELD) : docId;
		}
		return null;
	}
	
	public static void setId(Object document, String id) {
		if (isDocumentValid(document) && !callSetMethod(document, ID_SET_METHOD, id)) {
			setFieldValue(document, ID_FIELD, id);
		}
	}
	
	public static String getRev(Object document) {
		if (isDocumentValid(document)) {
			String docRev = callGetMethod(document, REV_GET_METHOD);
			return docRev == null ? getFieldValue(document, REV_FIELD) : docRev;
		}
		return null;
	}
	
	public static void setRev(Object document, String rev) {
		if (isDocumentValid(document) && !callSetMethod(document, REV_SET_METHOD, rev)) {
			setFieldValue(document, REV_FIELD, rev);
		}
	}
	
	private static String callGetMethod(Object document, String methodName) {
		try {
			Method method = document.getClass().getMethod(methodName);
			if (method != null) {
				Object value = method.invoke(document);
				if (value instanceof String) {
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
			if (method != null) {
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
	
	private static String getFieldValue(Object document, String fieldName) {
		try {
			Field field = document.getClass().getDeclaredField(fieldName);
			if (field != null && String.class.isAssignableFrom(field.getType())) {
				Object value = field.get(document);
				if (value instanceof String) {
					return (String) value;
				}
			}
		} catch (NoSuchFieldException e) {
		} catch (SecurityException e) {
		} catch (IllegalArgumentException e) {
		} catch (IllegalAccessException e) {
		}
		return null;
	}
	
	private static boolean setFieldValue(Object document, String fieldName, String value) {
		try {
			Field field = document.getClass().getDeclaredField(fieldName);
			if (field != null && String.class.isAssignableFrom(field.getType())) {
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
	
	private static boolean isDocumentValid(Object document) {
		return document != null && !(document instanceof InputStream);
	}
}
