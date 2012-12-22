package com.amebame.triton.json;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class ISO8601DateSerializer extends StdSerializer<Date> {

	public ISO8601DateSerializer() {
		this(Date.class);
	}

	protected ISO8601DateSerializer(Class<Date> t) {
		super(t);
	}

	@Override
	public void serialize(Date date, JsonGenerator jsonGenerator,
			SerializerProvider serializerProvider) throws IOException,
			JsonGenerationException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		jsonGenerator.writeString(sdf.format(date));
	}

}
