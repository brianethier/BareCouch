package ca.barelabs.barecouch;

@SuppressWarnings("serial")
public class DatabaseAccessException extends RuntimeException {

	public DatabaseAccessException() {
	}

	public DatabaseAccessException(String message) {
		super(message);
	}

	public DatabaseAccessException(Throwable throwable) {
		super(throwable);
	}
	
}
