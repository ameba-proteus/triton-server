/**
 * 
 */
package com.amebame.triton.json;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class ISO8601DateDeserializer extends StdDeserializer<Date> {

	private static final long serialVersionUID = -4225665718539636435L;

	public ISO8601DateDeserializer() {
		super(Date.class);
	}

	@Override
	public Date deserialize(JsonParser jsonParser,
			DeserializationContext deserializationContext) throws IOException,
			JsonProcessingException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		try {
			return sdf.parse(jsonParser.getText());
		} catch (ParseException e) {
			throw new JsonParseException("can't yyyy-MM-dd deserialize"
					+ jsonParser.getText(), jsonParser.getCurrentLocation(), e);
		}
	}

}
