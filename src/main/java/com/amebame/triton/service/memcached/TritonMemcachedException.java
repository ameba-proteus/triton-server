package com.amebame.triton.service.memcached;

import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.exception.TritonRuntimeException;

public class TritonMemcachedException extends TritonRuntimeException {

	private static final long serialVersionUID = 4640532692201844509L;

	public TritonMemcachedException() {
	}

	public TritonMemcachedException(TritonErrors error, String message) {
		super(error, message);
	}

	public TritonMemcachedException(TritonErrors error, Throwable cause) {
		super(error, cause.getMessage(), cause);
	}

	public TritonMemcachedException(TritonErrors error, String message, Throwable cause) {
		super(error, message, cause);
	}

}
