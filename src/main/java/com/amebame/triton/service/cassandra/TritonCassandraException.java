package com.amebame.triton.service.cassandra;

import com.amebame.triton.exception.TritonRuntimeException;

public class TritonCassandraException extends TritonRuntimeException {

	private static final long serialVersionUID = 7118320011419965473L;

	public TritonCassandraException() {
	}

	public TritonCassandraException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TritonCassandraException(String message, Throwable cause) {
		super(message, cause);
	}

	public TritonCassandraException(String message) {
		super(message);
	}

	public TritonCassandraException(Throwable cause) {
		super(cause);
	}

}
