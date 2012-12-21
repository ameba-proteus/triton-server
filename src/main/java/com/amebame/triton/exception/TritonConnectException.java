package com.amebame.triton.exception;

public class TritonConnectException extends TritonException {
	
	private static final long serialVersionUID = 6361241566200424L;

	public TritonConnectException() {
	}

	public TritonConnectException(String message) {
		super(message);
	}

	public TritonConnectException(Throwable cause) {
		super(cause);
	}

	public TritonConnectException(String message, Throwable cause) {
		super(message, cause);
	}

	public TritonConnectException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
