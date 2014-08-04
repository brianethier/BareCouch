package com.barenode.barecouch;


import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.barenode.bareconnection.RestConnection;
import com.barenode.bareconnection.RestException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class ViewIterator {

    private final RestConnection mConnection;
    private final Gson mGson;
    private final JsonReader mJsonReader;


    public ViewIterator(RestConnection connection) throws RestException {
        try {
            mConnection = connection;
            mGson = new Gson();
            mJsonReader = new JsonReader(new InputStreamReader(new BufferedInputStream(mConnection.get(InputStream.class)), "UTF-8"));
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
            throw new RestException(e);
        }
    }


    public boolean hasNext() throws RestException {
        try {
            return mJsonReader.hasNext();
        }
        catch(IOException e) {
            throw new RestException(e);
        }
    }

    public <T>T next(Class<T> clss) throws RestException {
        try {
            T value = null;
            mJsonReader.beginObject();
            while(mJsonReader.hasNext()) {
                String nextName = mJsonReader.nextName();
                if (nextName.equals("value")) {
                    value = (T) mGson.fromJson(mJsonReader, clss);
                } else {
                    mJsonReader.skipValue();
                }
            }
            mJsonReader.endObject();
            return value;
        }
        catch(IOException e) {
            throw new RestException(e);
        }
    }

    public void close() throws RestException {
        try {
            mJsonReader.endArray();
            while(mJsonReader.hasNext()) {
                mJsonReader.skipValue();
            }
            mJsonReader.endObject();
            mConnection.disconnect();
        }
        catch(IOException e) {
            throw new RestException(e);
        }
    }
}
