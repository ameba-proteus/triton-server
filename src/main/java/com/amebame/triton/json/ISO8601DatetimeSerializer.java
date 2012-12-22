package com.amebame.triton.json;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class ISO8601DatetimeSerializer extends StdSerializer<Date> {

	public ISO8601DatetimeSerializer() {
		this(Date.class);
	}

	protected ISO8601DatetimeSerializer(Class<Date> t) {
		super(t);
	}

	@Override
	public void serialize(Date date, JsonGenerator jsonGenerator,
			SerializerProvider serializerProvider) throws IOException,
			JsonGenerationException {
		SimpleDateFormat iso8601Format = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ssZ");
		iso8601Format.setTimeZone(TimeZone.getTimeZone("UTC"));
		jsonGenerator.writeString(iso8601Format.format(date));
	}

}
