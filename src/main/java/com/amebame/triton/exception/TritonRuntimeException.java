package com.amebame.triton.exception;

public class TritonRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7958464770082357800L;

	public TritonRuntimeException() {
		super();
	}

	public TritonRuntimeException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TritonRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public TritonRuntimeException(String message) {
		super(message);
	}

	public TritonRuntimeException(Throwable cause) {
		super(cause);
	}

}
