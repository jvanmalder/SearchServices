/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.alfresco.solr.stream;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 6.0.0
 */
public class AlfrescoExceptionStream extends TupleStream {

    private TupleStream stream;
    private Throwable openException;
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public AlfrescoExceptionStream(TupleStream stream) {
        this.stream = stream;
    }

    public List<TupleStream> children() {
        return null;
    }

    public void open() {
        try {
            stream.open();
        } catch (Throwable e) {
            this.openException = e;
        }
    }

    public Tuple read() {
        if(openException != null) {
            //There was an exception during the open.
            Map fields = new HashMap();
            fields.put("EXCEPTION", rootMessage(openException));
            fields.put("EOF", true);
            SolrException.log(log, openException);
            return new Tuple(fields);
        }

        try {
            return stream.read();
        } catch (Throwable e) {
            Map fields = new HashMap();
            fields.put("EXCEPTION", rootMessage(e));
            fields.put("EOF", true);
            SolrException.log(log, e);
            return new Tuple(fields);
        }
    }

    private String rootMessage(Throwable t) {
        while(t.getCause() != null) {
            t = t.getCause();
        }

        return t.getMessage();
    }


    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

        return new StreamExplanation(getStreamNodeId().toString())
            .withFunctionName("non-expressible")
            .withImplementingClass(this.getClass().getName())
            .withExpressionType(ExpressionType.STREAM_SOURCE)
            .withExpression("non-expressible");
    }

    public StreamComparator getStreamSort() {
        return this.stream.getStreamSort();
    }

    public void close() throws IOException {
        stream.close();
    }

    public void setStreamContext(StreamContext context) {
        this.stream.setStreamContext(context);
    }
}
