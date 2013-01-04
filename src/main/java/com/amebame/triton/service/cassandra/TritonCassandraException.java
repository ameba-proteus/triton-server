package com.amebame.triton.service.cassandra;

import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.exception.TritonRuntimeException;

public class TritonCassandraException extends TritonRuntimeException {

	private static final long serialVersionUID = 7118320011419965473L;

	public TritonCassandraException() {
	}

	public TritonCassandraException(TritonErrors error, String message, Throwable cause) {
		super(error, message, cause);
	}

	public TritonCassandraException(TritonErrors error, String message) {
		super(error, message);
	}

	public TritonCassandraException(TritonErrors error, Throwable cause) {
		super(error, cause.getMessage(), cause);
	}

}
