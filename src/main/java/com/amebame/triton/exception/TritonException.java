package com.amebame.triton.exception;

public class TritonException extends Exception {

	private static final long serialVersionUID = 6884306643710263861L;

	public TritonException() {
	}

	public TritonException(String message) {
		super(message);
	}

	public TritonException(Throwable cause) {
		super(cause);
	}

	public TritonException(String message, Throwable cause) {
		super(message, cause);
	}

	public TritonException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
