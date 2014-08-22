package com.barenode.barecouch;


import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.barenode.bareconnection.RestConnection;
import com.barenode.bareconnection.RestException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class ViewResult {

    private final RestConnection mConnection;
    private final JsonReader mJsonReader;


    public ViewResult(RestConnection connection) throws RestException {
        try {
            mConnection = connection;
            mJsonReader = new JsonReader(new InputStreamReader(new BufferedInputStream(mConnection.get(InputStream.class)), RestConnection.CHARSET));
            mJsonReader.beginObject();
            while (mJsonReader.hasNext()) {
                String name = mJsonReader.nextName();
                if (name.equals("rows")) {
                    mJsonReader.beginArray();
                    break;
                } else {
                    mJsonReader.skipValue();
                }
            }
        }
        catch(IOException e) {
            throw new RestException(connection.getResponseCode(), e);
        }
    }


    public <T> ViewIterator<T> iterator(Class<T> clss) {
        return new ViewIterator<T>(mJsonReader, clss);
    }

    public void close() {
        try {
			mJsonReader.endArray();
	        while(mJsonReader.hasNext()) {
	            mJsonReader.skipValue();
	        }
	        mJsonReader.endObject();
		} catch (IOException e) {
			
		}
        finally {
            mConnection.disconnect();
        }
    }
}
