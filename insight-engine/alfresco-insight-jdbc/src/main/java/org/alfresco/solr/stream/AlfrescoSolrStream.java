package org.alfresco.solr.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.JSONTupleStream;
import org.apache.solr.client.solrj.io.stream.JavabinTupleStreamParser;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.TupleStreamParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;

public class AlfrescoSolrStream extends SolrStream
{
    private static final long serialVersionUID = -6448762865539714148L;

    //In parent too but private
    private SolrParams params;
    private transient HttpSolrClient client;
    private transient TupleStreamParser tupleStreamParser;

    public AlfrescoSolrStream(String baseUrl, SolrParams params)
    {
        super(baseUrl, params);
        this.params = params;
    }

    /**
     * Opens the stream to a single Solr instance.
     **/
    @Override
    public void open() throws IOException
    {
        client  = new HttpSolrClient.Builder(getBaseUrl()).build();

        try {
            tupleStreamParser = constructAlfrescoParser(client, params);
        } catch (Exception e) {
            throw new IOException("params " + params, e);
        }
    }

    /**
     *  Closes the Stream to a single Solr Instance
     * */
    @Override
    public void close() throws IOException {

        if (tupleStreamParser != null) {
            tupleStreamParser.close();
        }

//        if(cache == null) {
//            client.close();
//        }
    }

    /**
     * Reads a Tuple from the stream. The Stream is completed when Tuple.EOF == true.
     **/
    @Override
    public Tuple read() throws IOException {
        try {
            Map<String, Object> fields = tupleStreamParser.next();

            if (fields == null) {
                //Return the EOF tuple.
                Map<String, Object> m = new HashMap<>();
                m.put("EOF", true);
                return new Tuple(m);
            } else {

                String msg = (String) fields.get("EXCEPTION");
                if (msg != null) {
                    HandledException ioException = new HandledException(msg);
                    throw ioException;
                }

//                if (trace) {
//                    fields.put("_CORE_", this.baseUrl);
//                    if(slice != null) {
//                        fields.put("_SLICE_", slice);
//                    }
//                }
//
//                if (fieldMappings != null) {
//                    fields = mapFields(fields, fieldMappings);
//                }
                return new Tuple(fields);
            }
        } catch (HandledException e) {
            throw new IOException("--> "+this.getBaseUrl()+":"+e.getMessage());
        } catch (Exception e) {
            //The Stream source did not provide an exception in a format that the SolrStream could propagate.
            throw new IOException("--> "+this.getBaseUrl()+": An exception has occurred on the server, refer to server log for details.", e);
        }
    }

    public static TupleStreamParser constructAlfrescoParser(SolrClient server, SolrParams requestParams) throws IOException, SolrServerException
    {
        String p = requestParams.get(CommonParams.QT);
        if (p != null) {
            ModifiableSolrParams modifiableSolrParams = (ModifiableSolrParams) requestParams;
            modifiableSolrParams.remove(CommonParams.QT);
        }

        String wt = requestParams.get(CommonParams.WT, "json");
        String alfrescoJson = "{\"tenants\":[\"\"],\"locales\":[\"en_US\"],\"defaultNamespace\":\"http://www.alfresco.org/model/content/1.0\",\"textAttributes\":[],\"defaultFTSOperator\":\"OR\",\"defaultFTSFieldOperator\":\"OR\",\"anyDenyDenies\":true,\"query\":\"name:*\",\"templates\":[],\"allAttributes\":[],\"queryConsistency\":\"DEFAULT\",\"authorities\":[\"GROUP_EVERYONE\",\"ROLE_ADMINISTRATOR\",\"ROLE_AUTHENTICATED\",\"admin\"]}";

        ModifiableSolrParams modifiableSolrParams = (ModifiableSolrParams)requestParams;
        modifiableSolrParams.set(FacetParams.FACET, "true");
        modifiableSolrParams.set(CommonParams.FQ, "{!afts}AUTHORITY_FILTER_FROM_JSON");
        modifiableSolrParams.set("includeMetadata", "true");

        QueryRequest query = new AlfrescoQueryRequest(alfrescoJson, requestParams);
        query.setPath(p);
        query.setResponseParser(new InputStreamResponseParser(wt));
        query.setMethod(SolrRequest.METHOD.POST);

        NamedList<Object> genericResponse = server.request(query);
        InputStream stream = (InputStream) genericResponse.get("stream");
        if (CommonParams.JAVABIN.equals(wt)) {
            return new JavabinTupleStreamParser(stream, true);
        } else {
            InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
            return new JSONTupleStream(reader);
        }
    }

    public static class AlfrescoQueryRequest extends QueryRequest
    {
        private static final long serialVersionUID = -3922593865628592917L;
        private String json;

        public AlfrescoQueryRequest(String json, SolrParams params)
        {
            super(params);
            this.json =json;
        }

        public Collection<ContentStream> getContentStreams()
        {
            List<ContentStream> streams = new ArrayList<ContentStream>();
            streams.add(new ContentStreamBase.StringStream(json));
            return streams;
        }
    }
}
