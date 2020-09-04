package com.alice.aef.transport;

import java.io.Serializable;

/**
 * AEF response, can replace AEFResponse class, uses boolean flag to
 * indicate success (this can actually be extracted into payload)
 *
 * @param <T>
 */
@Deprecated //use AEFResponse
public class AEFResponseEvent<T> implements AEFMessage, Serializable {
	private T payload;
	private boolean success;

	public static <T> AEFResponseEvent<T> successResponse() {
		AEFResponseEvent<T> response = new AEFResponseEvent<>();
		response.setSuccess(true);
		return response;
	}

	public static <T> AEFResponseEvent<T> errorResponse() {
		AEFResponseEvent<T> response = new AEFResponseEvent<>();
		response.setSuccess(false);
		return response;
	}

	public T getPayload() {
		return payload;
	}

	@SuppressWarnings("unchecked")
	public <S> S getTypedPayload() {
		return (S) payload;
	}

	public void setPayload(T payload) {
		this.payload = payload;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public AEFResponseEvent<T> withPayload(T payload) {
		this.payload = payload;
		return this;
	}
}
