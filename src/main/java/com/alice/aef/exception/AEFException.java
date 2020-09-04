package com.alice.aef.exception;

public class AEFException extends RuntimeException {
	public AEFException(String message) {
		super(message);
	}

	public AEFException(String message, Throwable cause) {
		super(message, cause);
	}
}
