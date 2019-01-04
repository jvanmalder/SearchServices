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

import static java.util.Arrays.asList;

import static org.alfresco.solr.stream.AlfrescoStreamHandler.AFTS_AUTHORITY_FILTER_FROM_JSON;
import static org.junit.Assert.assertEquals;

import org.alfresco.solr.stream.AlfrescoStreamHandler.AlfrescoRequestFactory;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

/** Unit tests for {@link AlfrescoRequestFactory}. */
public class AlfrescoRequestFactoryTest
{
    /** Check that the ACL filter query is added if nothing is provided. */
    @Test
    public void checkFilterQueries_NotProvided()
    {
        AlfrescoRequestFactory alfrescoRequestFactory = new AlfrescoRequestFactory("{}");
        ModifiableSolrParams solrParams = new ModifiableSolrParams();

        // Call the method under test.
        alfrescoRequestFactory.getRequest(solrParams);

        assertEquals("Unexpected filter query.", asList(AFTS_AUTHORITY_FILTER_FROM_JSON), asList(solrParams.getParams(CommonParams.FQ)));
    }

    /** Check that the ACL filter query is added along with any existing filter queries. */
    @Test
    public void checkFilterQueries_FQProvided()
    {
        AlfrescoRequestFactory alfrescoRequestFactory = new AlfrescoRequestFactory("{\"filterQueries\": [\"fq1\", \"fq2\"]}");
        ModifiableSolrParams solrParams = new ModifiableSolrParams();

        // Call the method under test.
        alfrescoRequestFactory.getRequest(solrParams);

        assertEquals("Unexpected filter query.", asList(AFTS_AUTHORITY_FILTER_FROM_JSON, "fq1", "fq2"),
                    asList(solrParams.getParams(CommonParams.FQ)));
    }
}
