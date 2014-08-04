package com.barenode.barecouch;

public class Error {

	private String error;
	private String reason;

	
	public Error(String error, String reason) {
		this.error = error;
		this.reason = reason;
	}
	
	
	public String getError() {
		return error;
	}
	
	public String getReason() {
		return reason;
	}
	
}
