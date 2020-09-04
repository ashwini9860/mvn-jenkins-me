package com.alice.aef.transport;

import java.io.Serializable;

/**
 * Wrapper class for sending content across the AEF
 *
 * @param <T> the content type
 */
public class AEFResponse<T> implements AEFMessage, Serializable {
	private T payload;
	private String errorMessage;
	private boolean success;

	private AEFResponse() {}

	public static <T> AEFResponse<T> makeSuccessResponse(T payload) {
		AEFResponse<T> response = new AEFResponse<>();
		response.setPayload(payload);
		response.setSuccess(true);
		return response;
	}

	public static <T> AEFResponse<T> makeErrorResponse(T payload, String errorMessage) {
		AEFResponse<T> response = new AEFResponse<>();
		response.setPayload(payload);
		response.setErrorMessage(errorMessage);
		response.setSuccess(false);
		return response;
	}

	public static <T> AEFResponse<T> makeErrorResponse(String errorMessage) {
		AEFResponse<T> response = new AEFResponse<>();
		response.setErrorMessage(errorMessage);
		response.setSuccess(false);
		return response;
	}

	public T getPayload() {
		return payload;
	}

	private void setPayload(T payload) {
		this.payload = payload;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	private void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	@Override
	public String toString() {
		return getClass().getName() + " " + (isSuccess() ? getPayload() : getErrorMessage());
	}
}