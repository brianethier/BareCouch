package com.barenode.barecouch;

@SuppressWarnings("serial")
public class DbAccessException extends RuntimeException {

	public DbAccessException() {
	}

	public DbAccessException(String message) {
		super(message);
	}

	public DbAccessException(Throwable throwable) {
		super(throwable);
	}
	
}
