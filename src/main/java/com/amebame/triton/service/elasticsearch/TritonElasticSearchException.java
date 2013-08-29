package com.amebame.triton.service.elasticsearch;

import com.amebame.triton.exception.TritonErrors;
import com.amebame.triton.exception.TritonRuntimeException;

public class TritonElasticSearchException extends TritonRuntimeException {

	private static final long serialVersionUID = 552457169092704590L;

	public TritonElasticSearchException() {
	}

	public TritonElasticSearchException(TritonErrors error, String message) {
		super(error, message);
	}

	public TritonElasticSearchException(TritonErrors error, Throwable cause) {
		super(error, cause.getMessage(), cause);
	}

	public TritonElasticSearchException(TritonErrors error, String message, Throwable cause) {
		super(error, message, cause);
	}

}
