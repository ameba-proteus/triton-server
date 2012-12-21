package com.amebame.triton.exception;

public class TritonJsonException extends TritonRuntimeException {

	private static final long serialVersionUID = -5916693574413964335L;

	public TritonJsonException() {
		super();
	}

	public TritonJsonException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TritonJsonException(String message, Throwable cause) {
		super(message, cause);
	}

	public TritonJsonException(String message) {
		super(message);
	}

	public TritonJsonException(Throwable cause) {
		super(cause);
	}

}
