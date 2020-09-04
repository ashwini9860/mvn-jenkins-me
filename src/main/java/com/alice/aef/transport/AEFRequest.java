package com.alice.aef.transport;

import java.io.Serializable;

/**
 * Generic class for encapsulating a request to pass through the event fabric
 *
 * @param <T> the request type
 */
public class AEFRequest<T> implements AEFMessage, Serializable {
	private T requestObject;

	public AEFRequest(T requestObject) {
		this.requestObject = requestObject;
	}

	public T getRequestObject() {
		return requestObject;
	}

	@Override
	public String toString() {
		return getClass().getName() + " " + requestObject;
	}
}
