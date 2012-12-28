package com.amebame.triton.service.memcached;

import com.amebame.triton.exception.TritonRuntimeException;

public class TritonMemcachedException extends TritonRuntimeException {

	private static final long serialVersionUID = 4640532692201844509L;

	public TritonMemcachedException() {
	}

	public TritonMemcachedException(String message) {
		super(message);
	}

	public TritonMemcachedException(Throwable cause) {
		super(cause.getMessage(), cause);
	}

	public TritonMemcachedException(String message, Throwable cause) {
		super(message, cause);
	}

	public TritonMemcachedException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
