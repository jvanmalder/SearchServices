/*
 * Copyright (C) 2005-2014 Alfresco Software Limited.
 *
 * This file is part of Alfresco
 *
 * Alfresco is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Alfresco is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Alfresco. If not, see <http://www.gnu.org/licenses/>.
 */
package org.alfresco.solr;


import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.*;

import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.ParserConfigurationException;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.search.impl.parsers.FTSQueryParser;
import org.alfresco.repo.tenant.TenantService;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.Period;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.service.cmr.repository.datatype.DefaultTypeConverter;
import org.alfresco.service.cmr.repository.datatype.Duration;
import org.alfresco.service.cmr.search.SearchParameters;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.alfresco.solr.client.*;
import org.alfresco.util.GUID;
import org.alfresco.util.ISO9075;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestHarness;
import org.apache.solr.util.TestHarness.TestCoresLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import static org.alfresco.repo.search.adaptor.lucene.QueryConstants.*;
import static org.alfresco.repo.search.adaptor.lucene.QueryConstants.FIELD_ISNODE;
import static org.alfresco.repo.search.adaptor.lucene.QueryConstants.FIELD_TENANT;


/**
 * @author Joel
 *
 */
public class AlfrescoSolrTestCaseJ4 extends SolrTestCaseJ4 implements SolrTestFiles
{
    private static long id = 0;
    private static int orderTextCount = 0;


    protected static NodeRef testRootNodeRef;
    protected static NodeRef testNodeRef;
    protected static NodeRef testBaseFolderNodeRef;
    protected static NodeRef testFolder00NodeRef;


    protected static NodeRef testCMISContent00NodeRef;
    protected static NodeRef testCMISRootNodeRef;
    protected static NodeRef testCMISBaseFolderNodeRef;
    protected static NodeRef testCMISFolder00NodeRef;
    protected static QName testCMISBaseFolderQName;
    protected static QName testCMISFolder00QName;
    protected static Date testCMISDate00;
    private static Date orderDate = new Date();
    protected static final String TEST_NAMESPACE = "http://www.alfresco.org/test/solrtest";

    protected static QName createdDate = QName.createQName(TEST_NAMESPACE, "createdDate");

    protected static QName createdTime = QName.createQName(TEST_NAMESPACE, "createdTime");

    protected static QName orderDouble = QName.createQName(TEST_NAMESPACE, "orderDouble");

    protected static QName orderFloat = QName.createQName(TEST_NAMESPACE, "orderFloat");

    protected static QName orderLong = QName.createQName(TEST_NAMESPACE, "orderLong");

    protected static QName orderInt = QName.createQName(TEST_NAMESPACE, "orderInt");

    protected static QName orderText = QName.createQName(TEST_NAMESPACE, "orderText");

    protected static QName orderLocalisedText = QName.createQName(TEST_NAMESPACE, "orderLocalisedText");

    protected static QName orderMLText = QName.createQName(TEST_NAMESPACE, "orderMLText");

    protected static QName orderLocalisedMLText = QName.createQName(TEST_NAMESPACE, "orderLocalisedMLText");

    protected static QName testSuperType = QName.createQName(TEST_NAMESPACE, "testSuperType");

    protected static QName testType = QName.createQName(TEST_NAMESPACE, "testType");

    protected static QName testAspect = QName.createQName(TEST_NAMESPACE, "testAspect");

    private static final String CMIS_TEST_NAMESPACE = "http://www.alfresco.org/test/cmis-query-test";

    protected static QName extendedContent = QName.createQName(CMIS_TEST_NAMESPACE, "extendedContent");

    protected static QName singleTextBoth = QName.createQName(CMIS_TEST_NAMESPACE, "singleTextBoth");

    protected static QName singleTextUntokenised = QName.createQName(CMIS_TEST_NAMESPACE, "singleTextUntokenised");

    protected static QName singleTextTokenised = QName.createQName(CMIS_TEST_NAMESPACE, "singleTextTokenised");

    protected static QName multipleTextBoth = QName.createQName(CMIS_TEST_NAMESPACE, "multipleTextBoth");

    protected static QName multipleTextUntokenised = QName.createQName(CMIS_TEST_NAMESPACE, "multipleTextUntokenised");

    protected static QName multipleTextTokenised = QName.createQName(CMIS_TEST_NAMESPACE, "multipleTextTokenised");

    protected static QName singleMLTextBoth = QName.createQName(CMIS_TEST_NAMESPACE, "singleMLTextBoth");

    protected static QName singleMLTextUntokenised = QName.createQName(CMIS_TEST_NAMESPACE, "singleMLTextUntokenised");

    protected static QName singleMLTextTokenised = QName.createQName(CMIS_TEST_NAMESPACE, "singleMLTextTokenised");

    protected static QName multipleMLTextBoth = QName.createQName(CMIS_TEST_NAMESPACE, "multipleMLTextBoth");

    protected static QName multipleMLTextUntokenised = QName.createQName(CMIS_TEST_NAMESPACE, "multipleMLTextUntokenised");

    protected static QName multipleMLTextTokenised = QName.createQName(CMIS_TEST_NAMESPACE, "multipleMLTextTokenised");

    protected static QName singleFloat = QName.createQName(CMIS_TEST_NAMESPACE, "singleFloat");

    protected static QName multipleFloat = QName.createQName(CMIS_TEST_NAMESPACE, "multipleFloat");

    protected static QName singleDouble = QName.createQName(CMIS_TEST_NAMESPACE, "singleDouble");

    protected static QName multipleDouble = QName.createQName(CMIS_TEST_NAMESPACE, "multipleDouble");

    protected static QName singleInteger = QName.createQName(CMIS_TEST_NAMESPACE, "singleInteger");

    protected static QName multipleInteger = QName.createQName(CMIS_TEST_NAMESPACE, "multipleInteger");

    protected static QName singleLong = QName.createQName(CMIS_TEST_NAMESPACE, "singleLong");

    protected static QName multipleLong = QName.createQName(CMIS_TEST_NAMESPACE, "multipleLong");

    protected static QName singleBoolean = QName.createQName(CMIS_TEST_NAMESPACE, "singleBoolean");

    protected static QName multipleBoolean = QName.createQName(CMIS_TEST_NAMESPACE, "multipleBoolean");

    protected static QName singleDate = QName.createQName(CMIS_TEST_NAMESPACE, "singleDate");

    protected static QName multipleDate = QName.createQName(CMIS_TEST_NAMESPACE, "multipleDate");

    protected static QName singleDatetime = QName.createQName(CMIS_TEST_NAMESPACE, "singleDatetime");

    protected static QName multipleDatetime = QName.createQName(CMIS_TEST_NAMESPACE, "multipleDatetime");

    private static String[] orderNames = new String[] { "one", "two", "three", "four", "five", "six", "seven", "eight",
            "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen" };

    // Spanish- Eng, French-English, Swedish German, English
    private static String[] orderLocalisedNames = new String[] { "chalina", "curioso", "llama", "luz", "peach", "péché",
            "pêche", "sin", "\u00e4pple", "banan", "p\u00e4ron", "orange", "rock", "rôle", "rose", "filler" };

    private static String[] orderLocaliseMLText_de = new String[] { "Arg", "Ärgerlich", "Arm", "Assistent", "Aßlar",
            "Assoziation", "Udet", "Übelacker", "Uell", "Ülle", "Ueve", "Üxküll", "Uffenbach", "apple", "and",
            "aardvark" };

    private static String[] orderLocaliseMLText_fr = new String[] { "cote", "côte", "coté", "côté", "rock", "lemur",
            "lemonade", "lemon", "kale", "guava", "cheese", "beans", "bananana", "apple", "and", "aardvark" };

    private static String[] orderLocaliseMLText_en = new String[] { "zebra", "tiger", "rose", "rôle", "rock", "lemur",
            "lemonade", "lemon", "kale", "guava", "cheese", "beans", "bananana", "apple", "and", "aardvark" };

    private static String[] orderLocaliseMLText_es = new String[] { "radio", "ráfaga", "rana", "rápido", "rastrillo", "arroz",
            "campo", "chihuahua", "ciudad", "limonada", "llaves", "luna", "bananana", "apple", "and", "aardvark" };

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected static AlfrescoSolrDataModel dataModel = AlfrescoSolrDataModel.getInstance();


    public static File HOME() {
        return getFile(TEST_FILES_LOCATION);
    }



    public static class SolrServletRequest extends SolrQueryRequestBase {
        public SolrServletRequest(SolrCore core, HttpServletRequest req)
        {
            super(core, new MultiMapSolrParams(Collections.<String, String[]> emptyMap()));
        }
    }

    public SolrServletRequest areq(ModifiableSolrParams params, String json) {
        if(params.get("wt" ) == null) params.add("wt","xml");
        SolrServletRequest req =  new SolrServletRequest(h.getCore(), null);
        req.setParams(params);
        if(json != null) {
            ContentStream stream = new ContentStreamBase.StringStream(json);
            ArrayList<ContentStream> streams = new ArrayList<ContentStream>();
            streams.add(stream);
            req.setContentStreams(streams);
        }
        return req;
    }

    /*
    @AfterClass
    public static void close() {
        System.out.println("################ Closing Core !!!! #########");
        h.close();
    }
    */

    public static void initAlfrescoCore(String config, String schema) throws Exception {
        log.info("##################################### init Alfresco core ##############");
        log.info("####initCore");

        configString = config;
        schemaString = schema;
        testSolrHome = HOME().toPath();
        ignoreException("ignore_exception");

        System.setProperty("solr.directoryFactory","solr.RAMDirectoryFactory");
        System.setProperty("solr.solr.home", testSolrHome.toAbsolutePath().toString());

        System.setProperty("solr.tests.maxBufferedDocs", "1000");
        System.setProperty("solr.tests.maxIndexingThreads", "10");
        System.setProperty("solr.tests.ramBufferSizeMB", "1024");
        

        // other  methods like starting a jetty instance need these too
        System.setProperty("solr.test.sys.prop1", "propone");
        System.setProperty("solr.test.sys.prop2", "proptwo");
        System.setProperty("alfresco.test", "true");

        String configFile = getSolrConfigFile();
        if (configFile != null) {
            createAlfrescoCore(config, schema);
        }
        log.info("####initCore end");
    }

    public static void createAlfrescoCore(String config, String schema) throws ParserConfigurationException, IOException, SAXException {
        assertNotNull(testSolrHome);
        
        Properties properties = new Properties();
        properties.put("solr.tests.maxBufferedDocs", "1000");
        properties.put("solr.tests.maxIndexingThreads", "10");
        properties.put("solr.tests.ramBufferSizeMB", "1024");
        properties.put("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
        properties.put("solr.tests.mergePolicy", "org.apache.lucene.index.TieredMergePolicy");
        
        CoreContainer coreContainer = new CoreContainer(TEST_FILES_LOCATION);
        SolrResourceLoader resourceLoader = new SolrResourceLoader(Paths.get(TEST_SOLR_CONF), null, properties);
        SolrConfig solrConfig = new SolrConfig(resourceLoader, config, null);
        IndexSchema indexSchema = IndexSchemaFactory.buildIndexSchema(schema, solrConfig);
        //CoreDescriptor coreDescriptor = new CoreDescriptor(coreContainer, SolrTestCaseJ4.DEFAULT_TEST_CORENAME, SolrTestCaseJ4.DEFAULT_TEST_CORENAME);
        
        TestCoresLocator locator = new TestCoresLocator(SolrTestCaseJ4.DEFAULT_TEST_CORENAME, "data", solrConfig.getResourceName(), indexSchema.getResourceName());
        
        NodeConfig nodeConfig = new NodeConfig.NodeConfigBuilder("name", coreContainer.getResourceLoader())
        .setUseSchemaCache(false)
        .setCoreAdminHandlerClass("org.alfresco.solr.AlfrescoCoreAdminHandler")
        .build();

        h = new TestHarness(nodeConfig, locator);
        //h.getCoreContainer().create(coreDescriptor);
        h.coreName = SolrTestCaseJ4.DEFAULT_TEST_CORENAME;
        
        lrf = h.getRequestFactory
                ("standard",0,20, CommonParams.VERSION,"2.2");
        
        coreContainer.shutdown();
    }

    public synchronized Node getNode(Transaction txn, Acl acl, Node.SolrApiNodeStatus status)
    {
        ++id;
        Node node = new Node();
        node.setTxnId(txn.getId());
        node.setId(id);
        node.setAclId(acl.getId());
        node.setStatus(status);
        return node;
    }

    public NodeMetaData getNodeMetaData(Node node, Transaction txn, Acl acl, String owner, Set<NodeRef> ancestors, boolean createError)
    {
        NodeMetaData nodeMetaData = new NodeMetaData();
        nodeMetaData.setId(node.getId());
        nodeMetaData.setAclId(acl.getId());
        nodeMetaData.setTxnId(txn.getId());
        nodeMetaData.setOwner(owner);
        nodeMetaData.setAspects(new HashSet());
        nodeMetaData.setAncestors(ancestors);
        Map<QName, PropertyValue> props = new HashMap();
        props.put(ContentModel.PROP_IS_INDEXED, new StringPropertyValue("true"));
        props.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.US, 0l, "UTF-8", "text/plain", null));
        nodeMetaData.setProperties(props);
        //If create createError is true then we leave out the nodeRef which will cause an error
        if(!createError) {
            NodeRef nodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
            nodeMetaData.setNodeRef(nodeRef);
        }
        nodeMetaData.setType(QName.createQName(TEST_NAMESPACE, "testSuperType"));
        nodeMetaData.setAncestors(ancestors);
        nodeMetaData.setPaths(new ArrayList());
        nodeMetaData.setNamePaths(new ArrayList());
        return nodeMetaData;
    }


    public synchronized Acl getAcl(AclChangeSet aclChangeSet)
    {
        ++id;
        Acl acl = new Acl(aclChangeSet.getId(), id);
        return acl;
    }

    public synchronized AclChangeSet getAclChangeSet(int aclCount)
    {
        ++id;
        AclChangeSet aclChangeSet = new AclChangeSet(id, System.currentTimeMillis(), aclCount);
        return aclChangeSet;
    }

    public AclReaders getAclReaders(AclChangeSet aclChangeSet, Acl acl, List<String> readers, List<String> denied, String tenant)
    {
        if(tenant == null)
        {
            tenant = TenantService.DEFAULT_DOMAIN;
        }

        return new AclReaders(acl.getId(), readers, denied, aclChangeSet.getId(), tenant);
    }

    //Maintenance method
    public void reindexAclChangeSetId(long aclChangeSetId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "REINDEX",
                        CoreAdminParams.NAME, h.getCore().getName(),
                        "acltxid", Long.toString(aclChangeSetId)),
                resp);
    }


    public void indexAclChangeSet(AclChangeSet aclChangeSet, List<Acl> aclList, List<AclReaders> aclReadersList)
    {
        //First map the nodes to a transaction.
        SOLRAPIQueueClient.aclMap.put(aclChangeSet.getId(), aclList);

        //Next map a node to the NodeMetaData
        for(AclReaders aclReaders : aclReadersList)
        {
            SOLRAPIQueueClient.aclReadersMap.put(aclReaders.getId(), aclReaders);
        }

        //Next add the transaction to the queue

        SOLRAPIQueueClient.aclChangeSetQueue.add(aclChangeSet);
        System.out.println("SOLRAPIQueueClient.aclChangeSetQueue.size():" + SOLRAPIQueueClient.aclChangeSetQueue.size());
    }


    //Maintenance method
    public void purgeAclChangeSetId(long aclChangeSetId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "PURGE",
                        CoreAdminParams.NAME, h.getCore().getName(),
                        "acltxid", Long.toString(aclChangeSetId)),
                resp);
    }

    //Maintenance method
    public void purgeNodeId(long nodeId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "PURGE",
                        CoreAdminParams.NAME, h.getCore().getName(),
                        "nodeid", Long.toString(nodeId)),
                resp);
    }

    //Maintenance method
    public void purgeTransactionId(long txnId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "PURGE",
                        CoreAdminParams.NAME, h.getCore().getName(),
                        "txid", Long.toString(txnId)),
                resp);
    }

    public List<String> list(String... strings)
    {
        List<String> list = new ArrayList();
        for(String s : strings)
        {
            list.add(s);
        }
        return list;
    }

    public List<Acl> list(Acl... acls)
    {
        List<Acl> list = new ArrayList();
        for(Acl acl : acls)
        {
            list.add(acl);
        }
        return list;

    }

    public List<Node> list(Node... nodes)
    {
        List<Node> list = new ArrayList();
        for(Node node : nodes)
        {
            list.add(node);
        }
        return list;

    }

    public List<AclReaders> list(AclReaders... aclReaders)
    {
        List<AclReaders> list = new ArrayList();
        for(AclReaders a : aclReaders)
        {
            list.add(a);
        }
        return list;

    }


    public List<NodeMetaData> list(NodeMetaData... nodeMetaDatas)
    {
        List<NodeMetaData> list = new ArrayList();
        for(NodeMetaData nodeMetaData : nodeMetaDatas)
        {
            list.add(nodeMetaData);
        }
        return list;

    }

    public Set<NodeRef> ancestors(NodeRef... refs) {
        Set set = new HashSet();
        for(NodeRef ref : refs) {
            set.add(ref);
        }
        return set;
    }

    //Maintenance method
    public void retry() throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "RETRY",
                        CoreAdminParams.NAME, h.getCore().getName()),
                resp);
    }

    //Maintenance method
    public void reindexAclId(long aclId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "REINDEX",
                        CoreAdminParams.NAME, h.getCore().getName(),
                        "aclid", Long.toString(aclId)),
                resp);
    }

    //Maintenance method
    public void reindexTransactionId(long txnId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION,
                        "REINDEX",
                        CoreAdminParams.NAME,
                        h.getCore().getName(),
                        "txid", Long.toString(txnId)),
                resp);
    }

    //Maintenance method
    public void reindexNodeId(long nodeId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "REINDEX",
                        CoreAdminParams.NAME, h.getCore().getName(),
                        "nodeid", Long.toString(nodeId)),
                resp);
    }

    //Maintenance method
    public void indexAclId(long aclId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "INDEX",
                        CoreAdminParams.NAME, h.getCore().getName(),
                        "aclid", Long.toString(aclId)),
                resp);
    }

    public synchronized Transaction getTransaction(int deletes, int updates)
    {
        long txnId = id++;
        long txnCommitTime = System.currentTimeMillis();
        Transaction transaction = new Transaction();
        transaction.setCommitTimeMs(txnCommitTime);
        transaction.setId(txnId);
        transaction.setDeletes(deletes);
        transaction.setUpdates(updates);
        return transaction;
    }

    public void indexTransaction(Transaction transaction, List<Node> nodes, List<NodeMetaData> nodeMetaDatas)
    {
        //First map the nodes to a transaction.
        SOLRAPIQueueClient.nodeMap.put(transaction.getId(), nodes);

        //Next map a node to the NodeMetaData
        for(NodeMetaData nodeMetaData : nodeMetaDatas)
        {
            SOLRAPIQueueClient.nodeMetaDataMap.put(nodeMetaData.getId(), nodeMetaData);
        }

        //Next add the transaction to the queue
        SOLRAPIQueueClient.transactionQueue.add(transaction);
    }

    //Maintenance method
    public void purgeAclId(long aclId) throws Exception
    {
        CoreAdminHandler admin = h.getCoreContainer().getMultiCoreHandler();
        SolrQueryResponse resp = new SolrQueryResponse();
        admin.handleRequestBody(req(CoreAdminParams.ACTION, "PURGE",
                        CoreAdminParams.NAME, h.getCore().getName(),
                        "aclid", Long.toString(aclId)),
                resp);
    }

    protected static synchronized String createGUID()
    {
        long time;
        time = id++;
        return "00000000-0000-" + ((time / 1000000000000L) % 10000L) + "-" + ((time / 100000000L) % 10000L) + "-"
                + (time % 100000000L);
    }


    public void waitForDocCount(Query query, long expectedNumFound, long waitMillis)
            throws Exception
    {
        Date date = new Date();
        long timeout = (long)date.getTime() + waitMillis;

        RefCounted<SolrIndexSearcher> ref = null;
        int totalHits = 0;
        while(new Date().getTime() < timeout)
        {
            try
            {
                ref = h.getCore().getSearcher();
                SolrIndexSearcher searcher = ref.get();
                TopDocs topDocs = searcher.search(query, 10);
                totalHits = topDocs.totalHits;
                if (topDocs.totalHits == expectedNumFound)
                {
                    return;
                }
                else
                {
                    Thread.sleep(2000);
                }
            }
            finally
            {
                ref.decref();
            }
        }
        throw new Exception("Wait error expected "+expectedNumFound+" found "+totalHits+" : "+query.toString());
    }

    public static void loadCMISTestSet() throws IOException {
        SolrCore core = h.getCore();
        AlfrescoSolrDataModel dataModel = AlfrescoSolrDataModel.getInstance();
        dataModel.setCMDefaultUri();
        NodeRef rootNodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        testCMISRootNodeRef = rootNodeRef;
        addStoreRoot(core, dataModel, rootNodeRef, 1, 1, 1, 1);

        // Base

        HashMap<QName, PropertyValue> baseFolderProperties = new HashMap<QName, PropertyValue>();
        baseFolderProperties.put(ContentModel.PROP_NAME, new StringPropertyValue("Base Folder"));
        // This variable is never used. What was it meant to be used for?
        HashMap<QName, String> baseFolderContent = new HashMap<QName, String>();
        NodeRef baseFolderNodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        testCMISBaseFolderNodeRef = baseFolderNodeRef;
        QName baseFolderQName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "baseFolder");
        testCMISBaseFolderQName = baseFolderQName;
        ChildAssociationRef n01CAR = new ChildAssociationRef(ContentModel.ASSOC_CHILDREN, rootNodeRef,
                baseFolderQName, baseFolderNodeRef, true, 0);
        addNode(core, dataModel, 1, 2, 1, ContentModel.TYPE_FOLDER, null, baseFolderProperties, null, "andy",
                new ChildAssociationRef[] { n01CAR }, new NodeRef[] { rootNodeRef }, new String[] { "/"
                        + baseFolderQName.toString() }, baseFolderNodeRef, true);

        // Folders

        HashMap<QName, PropertyValue> folder00Properties = new HashMap<QName, PropertyValue>();
        folder00Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 0"));
        HashMap<QName, String> folder00Content = new HashMap<QName, String>();
        NodeRef folder00NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        testCMISFolder00NodeRef = folder00NodeRef;
        QName folder00QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 0");
        testCMISFolder00QName = folder00QName;
        ChildAssociationRef folder00CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, baseFolderNodeRef,
                folder00QName, folder00NodeRef, true, 0);
        addNode(core, dataModel, 1, 3, 1, ContentModel.TYPE_FOLDER, null, folder00Properties, null, "andy",
                new ChildAssociationRef[] { folder00CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() },
                folder00NodeRef, true);

        HashMap<QName, PropertyValue> folder01Properties = new HashMap<QName, PropertyValue>();
        folder01Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 1"));
        HashMap<QName, String> folder01Content = new HashMap<QName, String>();
        NodeRef folder01NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder01QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 1");
        ChildAssociationRef folder01CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, baseFolderNodeRef,
                folder01QName, folder01NodeRef, true, 0);
        addNode(core, dataModel, 1, 4, 1, ContentModel.TYPE_FOLDER, null, folder01Properties, null, "bob",
                new ChildAssociationRef[] { folder01CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder01QName.toString() },
                folder01NodeRef, true);

        HashMap<QName, PropertyValue> folder02Properties = new HashMap<QName, PropertyValue>();
        folder02Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 2"));
        HashMap<QName, String> folder02Content = new HashMap<QName, String>();
        NodeRef folder02NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder02QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 2");
        ChildAssociationRef folder02CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, baseFolderNodeRef,
                folder02QName, folder02NodeRef, true, 0);
        addNode(core, dataModel, 1, 5, 1, ContentModel.TYPE_FOLDER, null, folder02Properties, null, "cid",
                new ChildAssociationRef[] { folder02CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder02QName.toString() },
                folder02NodeRef, true);

        HashMap<QName, PropertyValue> folder03Properties = new HashMap<QName, PropertyValue>();
        folder03Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 3"));
        HashMap<QName, String> folder03Content = new HashMap<QName, String>();
        NodeRef folder03NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder03QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 3");
        ChildAssociationRef folder03CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, baseFolderNodeRef,
                folder03QName, folder03NodeRef, true, 0);
        addNode(core, dataModel, 1, 6, 1, ContentModel.TYPE_FOLDER, null, folder03Properties, null, "dave",
                new ChildAssociationRef[] { folder03CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder03QName.toString() },
                folder03NodeRef, true);

        HashMap<QName, PropertyValue> folder04Properties = new HashMap<QName, PropertyValue>();
        folder04Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 4"));
        HashMap<QName, String> folder04Content = new HashMap<QName, String>();
        NodeRef folder04NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder04QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 4");
        ChildAssociationRef folder04CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder00NodeRef,
                folder04QName, folder04NodeRef, true, 0);
        addNode(core, dataModel, 1, 7, 1, ContentModel.TYPE_FOLDER, null, folder04Properties, null, "eoin",
                new ChildAssociationRef[] { folder04CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef,
                        folder00NodeRef }, new String[] { "/" + baseFolderQName.toString() + "/"
                        + folder00QName.toString() + "/" + folder04QName.toString() }, folder04NodeRef,
                true);

        HashMap<QName, PropertyValue> folder05Properties = new HashMap<QName, PropertyValue>();
        folder05Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 5"));
        HashMap<QName, String> folder05Content = new HashMap<QName, String>();
        NodeRef folder05NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder05QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 5");
        ChildAssociationRef folder05CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder00NodeRef,
                folder05QName, folder05NodeRef, true, 0);
        addNode(core, dataModel, 1, 8, 1, ContentModel.TYPE_FOLDER, null, folder05Properties, null, "fred",
                new ChildAssociationRef[] { folder05CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef,
                        folder00NodeRef }, new String[] { "/" + baseFolderQName.toString() + "/"
                        + folder00QName.toString() + "/" + folder05QName.toString() }, folder05NodeRef,
                true);

        HashMap<QName, PropertyValue> folder06Properties = new HashMap<QName, PropertyValue>();
        folder06Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 6"));
        HashMap<QName, String> folder06Content = new HashMap<QName, String>();
        NodeRef folder06NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder06QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 6");
        ChildAssociationRef folder06CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder05NodeRef,
                folder06QName, folder06NodeRef, true, 0);
        addNode(core, dataModel, 1, 9, 1, ContentModel.TYPE_FOLDER, null, folder06Properties, null, "gail",
                new ChildAssociationRef[] { folder06CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef,
                        folder00NodeRef, folder05NodeRef }, new String[] { "/" + baseFolderQName.toString()
                        + "/" + folder00QName.toString() + "/" + folder05QName.toString() + "/"
                        + folder06QName.toString() }, folder06NodeRef, true);

        HashMap<QName, PropertyValue> folder07Properties = new HashMap<QName, PropertyValue>();
        folder07Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 7"));
        HashMap<QName, String> folder07Content = new HashMap<QName, String>();
        NodeRef folder07NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder07QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 7");
        ChildAssociationRef folder07CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder06NodeRef,
                folder07QName, folder07NodeRef, true, 0);
        addNode(core,
                dataModel,
                1,
                10,
                1,
                ContentModel.TYPE_FOLDER,
                null,
                folder07Properties,
                null,
                "hal",
                new ChildAssociationRef[] { folder07CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder05NodeRef,
                        folder06NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder05QName.toString() + "/" + folder06QName.toString() + "/"
                        + folder07QName.toString() }, folder07NodeRef, true);

        HashMap<QName, PropertyValue> folder08Properties = new HashMap<QName, PropertyValue>();
        folder08Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 8"));
        HashMap<QName, String> folder08Content = new HashMap<QName, String>();
        NodeRef folder08NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder08QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 8");
        ChildAssociationRef folder08CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder07NodeRef,
                folder08QName, folder08NodeRef, true, 0);
        addNode(core,
                dataModel,
                1,
                11,
                1,
                ContentModel.TYPE_FOLDER,
                null,
                folder08Properties,
                null,
                "ian",
                new ChildAssociationRef[] { folder08CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder05NodeRef,
                        folder06NodeRef, folder07NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder05QName.toString() + "/" + folder06QName.toString() + "/"
                        + folder07QName.toString() + "/" + folder08QName.toString() }, folder08NodeRef,
                true);

        HashMap<QName, PropertyValue> folder09Properties = new HashMap<QName, PropertyValue>();
        folder09Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Folder 9'"));
        HashMap<QName, String> folder09Content = new HashMap<QName, String>();
        NodeRef folder09NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName folder09QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Folder 9'");
        ChildAssociationRef folder09CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder08NodeRef,
                folder09QName, folder09NodeRef, true, 0);
        addNode(core,
                dataModel,
                1,
                12,
                1,
                ContentModel.TYPE_FOLDER,
                null,
                folder09Properties,
                null,
                "jake",
                new ChildAssociationRef[] { folder09CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder05NodeRef,
                        folder06NodeRef, folder07NodeRef, folder08NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder05QName.toString() + "/" + folder06QName.toString() + "/"
                        + folder07QName.toString() + "/" + folder08QName.toString() + "/"
                        + folder09QName.toString() }, folder09NodeRef, true);

        // content

        HashMap<QName, PropertyValue> content00Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc00 = new MLTextPropertyValue();
        desc00.addValue(Locale.ENGLISH, "Alfresco tutorial");
        desc00.addValue(Locale.US, "Alfresco tutorial");
        content00Properties.put(ContentModel.PROP_DESCRIPTION, desc00);
        content00Properties.put(ContentModel.PROP_TITLE, desc00);
        content00Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content00Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Alfresco Tutorial"));
        content00Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content00Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        content00Properties.put(ContentModel.PROP_VERSION_LABEL, new StringPropertyValue("1.0"));
        content00Properties.put(ContentModel.PROP_OWNER, new StringPropertyValue("andy"));
        Date date00 = new Date();
        testCMISDate00 = date00;

        content00Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date00)));
        content00Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date00)));
        HashMap<QName, String> content00Content = new HashMap<QName, String>();
        content00Content
                .put(ContentModel.PROP_CONTENT,
                        "The quick brown fox jumped over the lazy dog and ate the Alfresco Tutorial, in pdf format, along with the following stop words;  a an and are"
                                + " as at be but by for if in into is it no not of on or such that the their then there these they this to was will with: "
                                + " and random charcters \u00E0\u00EA\u00EE\u00F0\u00F1\u00F6\u00FB\u00FF score");
        NodeRef content00NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        testCMISContent00NodeRef = content00NodeRef;
        QName content00QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Alfresco Tutorial");
        ChildAssociationRef content00CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder00NodeRef,
                content00QName, content00NodeRef, true, 0);
        addNode(core, dataModel, 1, 13, 1, ContentModel.TYPE_CONTENT, new QName[] { ContentModel.ASPECT_OWNABLE,
                        ContentModel.ASPECT_TITLED }, content00Properties, content00Content, "andy",
                new ChildAssociationRef[] { content00CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef,
                        folder00NodeRef }, new String[] { "/" + baseFolderQName.toString() + "/"
                        + folder00QName.toString() + "/" + content00QName.toString() }, content00NodeRef,
                true);

        HashMap<QName, PropertyValue> content01Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc01 = new MLTextPropertyValue();
        desc01.addValue(Locale.ENGLISH, "One");
        desc01.addValue(Locale.US, "One");
        content01Properties.put(ContentModel.PROP_DESCRIPTION, desc01);
        content01Properties.put(ContentModel.PROP_TITLE, desc01);
        content01Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content01Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("AA%"));
        content01Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content01Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date01 = new Date(date00.getTime() + 1000);
        content01Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date01)));
        content01Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date01)));
        HashMap<QName, String> content01Content = new HashMap<QName, String>();
        content01Content.put(ContentModel.PROP_CONTENT, "One Zebra Apple score score score score score score score score score score score");
        NodeRef content01NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content01QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "AA%");
        ChildAssociationRef content01CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder01NodeRef,
                content01QName, content01NodeRef, true, 0);
        addNode(core, dataModel, 1, 14, 1, ContentModel.TYPE_CONTENT, new QName[] { ContentModel.ASPECT_TITLED },
                content01Properties, content01Content, "cmis", new ChildAssociationRef[] { content01CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder01NodeRef }, new String[] { "/"
                        + baseFolderQName.toString() + "/" + folder01QName.toString() + "/"
                        + content01QName.toString() }, content01NodeRef, true);

        HashMap<QName, PropertyValue> content02Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc02 = new MLTextPropertyValue();
        desc02.addValue(Locale.ENGLISH, "Two");
        desc02.addValue(Locale.US, "Two");
        content02Properties.put(ContentModel.PROP_DESCRIPTION, desc02);
        content02Properties.put(ContentModel.PROP_TITLE, desc02);
        content02Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content02Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("BB_"));
        content02Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content02Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date02 = new Date(date01.getTime() + 1000);
        content02Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date02)));
        content02Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date02)));
        HashMap<QName, String> content02Content = new HashMap<QName, String>();
        content02Content.put(ContentModel.PROP_CONTENT, "Two Zebra Banana score score score score score score score score score score pad");
        NodeRef content02NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content02QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "BB_");
        ChildAssociationRef content02CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder02NodeRef,
                content02QName, content02NodeRef, true, 0);
        addNode(core, dataModel, 1, 15, 1, ContentModel.TYPE_CONTENT, new QName[] { ContentModel.ASPECT_TITLED },
                content02Properties, content02Content, "cmis", new ChildAssociationRef[] { content02CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder02NodeRef }, new String[] { "/"
                        + baseFolderQName.toString() + "/" + folder02QName.toString() + "/"
                        + content02QName.toString() }, content02NodeRef, true);

        HashMap<QName, PropertyValue> content03Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc03 = new MLTextPropertyValue();
        desc03.addValue(Locale.ENGLISH, "Three");
        desc03.addValue(Locale.US, "Three");
        content03Properties.put(ContentModel.PROP_DESCRIPTION, desc03);
        content03Properties.put(ContentModel.PROP_TITLE, desc03);
        content03Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content03Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("CC\\"));
        content03Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content03Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date03 = new Date(date02.getTime() + 1000);
        content03Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date03)));
        content03Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date03)));
        HashMap<QName, String> content03Content = new HashMap<QName, String>();
        content03Content.put(ContentModel.PROP_CONTENT, "Three Zebra Clementine score score score score score score score score score pad pad");
        NodeRef content03NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content03QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "CC\\");
        ChildAssociationRef content03CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder03NodeRef,
                content03QName, content03NodeRef, true, 0);
        addNode(core, dataModel, 1, 16, 1, ContentModel.TYPE_CONTENT, new QName[] { ContentModel.ASPECT_TITLED },
                content03Properties, content03Content, "cmis", new ChildAssociationRef[] { content03CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder03NodeRef }, new String[] { "/"
                        + baseFolderQName.toString() + "/" + folder03QName.toString() + "/"
                        + content03QName.toString() }, content03NodeRef, true);

        HashMap<QName, PropertyValue> content04Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc04 = new MLTextPropertyValue();
        desc04.addValue(Locale.ENGLISH, "Four");
        desc04.addValue(Locale.US, "Four");
        content04Properties.put(ContentModel.PROP_DESCRIPTION, desc04);
        content04Properties.put(ContentModel.PROP_TITLE, desc04);
        content04Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content04Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("DD\'"));
        content04Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content04Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date04 = new Date(date03.getTime() + 1000);
        content04Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date04)));
        content04Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date04)));
        HashMap<QName, String> content04Content = new HashMap<QName, String>();
        content04Content.put(ContentModel.PROP_CONTENT, "Four zebra durian score score score score score score score score pad pad pad");
        NodeRef content04NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content04QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "DD\'");
        ChildAssociationRef content04CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder04NodeRef,
                content04QName, content04NodeRef, true, 0);
        addNode(core, dataModel, 1, 17, 1, ContentModel.TYPE_CONTENT, new QName[] { ContentModel.ASPECT_TITLED },
                content04Properties, content04Content, null, new ChildAssociationRef[] { content04CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder04NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder04QName.toString() + "/" + content04QName.toString() }, content04NodeRef,
                true);

        HashMap<QName, PropertyValue> content05Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc05 = new MLTextPropertyValue();
        desc05.addValue(Locale.ENGLISH, "Five");
        desc05.addValue(Locale.US, "Five");
        content05Properties.put(ContentModel.PROP_DESCRIPTION, desc05);
        content05Properties.put(ContentModel.PROP_TITLE, desc05);
        content05Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content05Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("EE.aa"));
        content05Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content05Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date05 = new Date(date04.getTime() + 1000);
        content05Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date05)));
        content05Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date05)));
        content05Properties.put(
                ContentModel.PROP_EXPIRY_DATE,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class,
                        DefaultTypeConverter.INSTANCE.convert(Date.class, "2012-12-12T12:12:12.012Z"))));
        content05Properties.put(ContentModel.PROP_LOCK_OWNER, new StringPropertyValue("andy"));
        content05Properties.put(ContentModel.PROP_LOCK_TYPE, new StringPropertyValue("WRITE_LOCK"));
        HashMap<QName, String> content05Content = new HashMap<QName, String>();
        content05Content.put(ContentModel.PROP_CONTENT, "Five zebra Ebury score score score score score score score pad pad pad pad");
        NodeRef content05NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content05QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "EE.aa");
        ChildAssociationRef content05CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder05NodeRef,
                content05QName, content05NodeRef, true, 0);
        addNode(core, dataModel, 1, 18, 1, ContentModel.TYPE_CONTENT, new QName[] { ContentModel.ASPECT_TITLED,
                        ContentModel.ASPECT_LOCKABLE }, content05Properties, content05Content, null,
                new ChildAssociationRef[] { content05CAR }, new NodeRef[] { baseFolderNodeRef, rootNodeRef,
                        folder00NodeRef, folder05NodeRef }, new String[] { "/" + baseFolderQName.toString()
                        + "/" + folder00QName.toString() + "/" + content05QName.toString() },
                content05NodeRef, true);

        HashMap<QName, PropertyValue> content06Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc06 = new MLTextPropertyValue();
        desc06.addValue(Locale.ENGLISH, "Six");
        desc06.addValue(Locale.US, "Six");
        content06Properties.put(ContentModel.PROP_DESCRIPTION, desc06);
        content06Properties.put(ContentModel.PROP_TITLE, desc06);
        content06Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content06Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("FF.EE"));
        content06Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content06Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date06 = new Date(date05.getTime() + 1000);
        content06Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date06)));
        content06Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date06)));
        HashMap<QName, String> content06Content = new HashMap<QName, String>();
        content06Content.put(ContentModel.PROP_CONTENT, "Six zebra fig score score score score score score pad pad pad pad pad");
        NodeRef content06NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content06QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "FF.EE");
        ChildAssociationRef content06CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder06NodeRef,
                content06QName, content06NodeRef, true, 0);
        addNode(core,
                dataModel,
                1,
                19,
                1,
                ContentModel.TYPE_CONTENT,
                new QName[] { ContentModel.ASPECT_TITLED },
                content06Properties,
                content06Content,
                null,
                new ChildAssociationRef[] { content06CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder05NodeRef,
                        folder06NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder05QName.toString() + "/" + folder06QName.toString() + "/"
                        + content06QName.toString() }, content06NodeRef, true);

        HashMap<QName, PropertyValue> content07Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc07 = new MLTextPropertyValue();
        desc07.addValue(Locale.ENGLISH, "Seven");
        desc07.addValue(Locale.US, "Seven");
        content07Properties.put(ContentModel.PROP_DESCRIPTION, desc07);
        content07Properties.put(ContentModel.PROP_TITLE, desc07);
        content07Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content07Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("GG*GG"));
        content07Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content07Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date07 = new Date(date06.getTime() + 1000);
        content07Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date07)));
        content07Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date07)));
        HashMap<QName, String> content07Content = new HashMap<QName, String>();
        content07Content.put(ContentModel.PROP_CONTENT, "Seven zebra grapefruit score score score score score pad pad pad pad pad pad");
        NodeRef content07NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content07QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "GG*GG");
        ChildAssociationRef content07CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder07NodeRef,
                content07QName, content07NodeRef, true, 0);
        addNode(core,
                dataModel,
                1,
                20,
                1,
                ContentModel.TYPE_CONTENT,
                new QName[] { ContentModel.ASPECT_TITLED },
                content07Properties,
                content07Content,
                null,
                new ChildAssociationRef[] { content07CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder05NodeRef,
                        folder06NodeRef, folder07NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder05QName.toString() + "/" + folder06QName.toString() + "/"
                        + folder07QName.toString() + "/" + content07QName.toString() }, content07NodeRef,
                true);

        HashMap<QName, PropertyValue> content08Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc08 = new MLTextPropertyValue();
        desc08.addValue(Locale.ENGLISH, "Eight");
        desc08.addValue(Locale.US, "Eight");
        content08Properties.put(ContentModel.PROP_DESCRIPTION, desc08);
        content08Properties.put(ContentModel.PROP_TITLE, desc08);
        content08Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-8",
                "text/plain", null));
        content08Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("HH?HH"));
        content08Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content08Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date08 = new Date(date07.getTime() + 1000);
        content08Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date08)));
        content08Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date08)));
        HashMap<QName, String> content08Content = new HashMap<QName, String>();
        content08Content.put(ContentModel.PROP_CONTENT, "Eight zebra jackfruit score score score score pad pad pad pad pad pad pad");
        NodeRef content08NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content08QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "HH?HH");
        ChildAssociationRef content08CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder08NodeRef,
                content08QName, content08NodeRef, true, 0);
        addNode(core,
                dataModel,
                1,
                21,
                1,
                ContentModel.TYPE_CONTENT,
                new QName[] { ContentModel.ASPECT_TITLED },
                content08Properties,
                content08Content,
                null,
                new ChildAssociationRef[] { content08CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder05NodeRef,
                        folder06NodeRef, folder07NodeRef, folder08NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder05QName.toString() + "/" + folder06QName.toString() + "/"
                        + folder07QName.toString() + "/" + folder08QName.toString() + "/"
                        + content08QName.toString() }, content08NodeRef, true);

        HashMap<QName, PropertyValue> content09Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc09 = new MLTextPropertyValue();
        desc09.addValue(Locale.ENGLISH, "Nine");
        desc09.addValue(Locale.US, "Nine");
        content09Properties.put(ContentModel.PROP_DESCRIPTION, desc09);
        content09Properties.put(ContentModel.PROP_TITLE, desc09);
        content09Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-9",
                "text/plain", null));
        content09Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("aa"));
        content09Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content09Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date09 = new Date(date08.getTime() + 1000);
        content09Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date09)));
        content09Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date09)));
        content09Properties.put(ContentModel.PROP_VERSION_LABEL, new StringPropertyValue("label"));
        HashMap<QName, String> content09Content = new HashMap<QName, String>();
        content09Content.put(ContentModel.PROP_CONTENT, "Nine zebra kiwi score score score pad pad pad pad pad pad pad pad");
        NodeRef content09NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content09QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "aa");
        ChildAssociationRef content09CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder09NodeRef,
                content09QName, content09NodeRef, true, 0);
        addNode(core,
                dataModel,
                1,
                22,
                1,
                ContentModel.TYPE_CONTENT,
                new QName[] { ContentModel.ASPECT_TITLED },
                content09Properties,
                content09Content,
                null,
                new ChildAssociationRef[] { content09CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder05NodeRef,
                        folder06NodeRef, folder07NodeRef, folder08NodeRef, folder09NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder05QName.toString() + "/" + folder06QName.toString() + "/"
                        + folder07QName.toString() + "/" + folder08QName.toString() + "/"
                        + folder09QName.toString() + "/" + content09QName.toString() }, content09NodeRef,
                true);

        HashMap<QName, PropertyValue> content10Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc10 = new MLTextPropertyValue();
        desc10.addValue(Locale.ENGLISH, "Ten");
        desc10.addValue(Locale.US, "Ten");
        content10Properties.put(ContentModel.PROP_DESCRIPTION, desc10);
        content10Properties.put(ContentModel.PROP_TITLE, desc10);
        content10Properties.put(ContentModel.PROP_CONTENT, new ContentPropertyValue(Locale.UK, 0l, "UTF-9",
                "text/plain", null));
        content10Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("aa-thumb"));
        content10Properties.put(ContentModel.PROP_CREATOR, new StringPropertyValue("System"));
        content10Properties.put(ContentModel.PROP_MODIFIER, new StringPropertyValue("System"));
        Date date10 = new Date(date09.getTime() + 1000);
        content10Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date10)));
        content10Properties.put(ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date10)));
        content10Properties.put(ContentModel.PROP_VERSION_LABEL, new StringPropertyValue("label"));
        HashMap<QName, String> content10Content = new HashMap<QName, String>();
        content10Content.put(ContentModel.PROP_CONTENT, "Ten zebra kiwi thumb score pad pad pad pad pad pad pad pad pad");
        NodeRef content10NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content10QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "aa-thumb");
        ChildAssociationRef content10CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder09NodeRef,
                content10QName, content10NodeRef, true, 0);
        addNode(core,
                dataModel,
                1,
                23,
                1,
                ContentModel.TYPE_DICTIONARY_MODEL,
                new QName[] { ContentModel.ASPECT_TITLED },
                content10Properties,
                content10Content,
                null,
                new ChildAssociationRef[] { content10CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef, folder05NodeRef,
                        folder06NodeRef, folder07NodeRef, folder08NodeRef, folder09NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/"
                        + folder05QName.toString() + "/" + folder06QName.toString() + "/"
                        + folder07QName.toString() + "/" + folder08QName.toString() + "/"
                        + folder09QName.toString() + "/" + content10QName.toString() }, content10NodeRef,
                true);
    }

    protected static void addTypeTestData(NodeRef folder00NodeRef,
                                          NodeRef rootNodeRef,
                                          NodeRef baseFolderNodeRef,
                                          Object baseFolderQName,
                                          Object folder00QName,
                                          Date date1)
            throws IOException
    {
        HashMap<QName, PropertyValue> content00Properties = new HashMap<QName, PropertyValue>();
        MLTextPropertyValue desc00 = new MLTextPropertyValue();
        desc00.addValue(Locale.ENGLISH, "Test One");
        desc00.addValue(Locale.US, "Test 1");
        content00Properties.put(ContentModel.PROP_DESCRIPTION, desc00);
        content00Properties.put(ContentModel.PROP_TITLE, desc00);
        content00Properties.put(ContentModel.PROP_NAME, new StringPropertyValue("Test One"));
        content00Properties.put(ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date1)));

        StringPropertyValue single = new StringPropertyValue("Un tokenised");
        content00Properties.put(singleTextUntokenised, single);
        content00Properties.put(singleTextTokenised, single);
        content00Properties.put(singleTextBoth, single);
        MultiPropertyValue multi = new MultiPropertyValue();
        multi.addValue(single);
        multi.addValue(new StringPropertyValue("two parts"));
        content00Properties.put(multipleTextUntokenised, multi);
        content00Properties.put(multipleTextTokenised, multi);
        content00Properties.put(multipleTextBoth, multi);
        content00Properties.put(singleMLTextUntokenised, makeMLText());
        content00Properties.put(singleMLTextTokenised, makeMLText());
        content00Properties.put(singleMLTextBoth, makeMLText());
        content00Properties.put(multipleMLTextUntokenised, makeMLTextMVP());
        content00Properties.put(multipleMLTextTokenised, makeMLTextMVP());
        content00Properties.put(multipleMLTextBoth, makeMLTextMVP());
        StringPropertyValue one = new StringPropertyValue("1");
        StringPropertyValue two = new StringPropertyValue("2");
        MultiPropertyValue multiDec = new MultiPropertyValue();
        multiDec.addValue(one);
        multiDec.addValue(new StringPropertyValue("1.1"));
        content00Properties.put(singleFloat, one);
        content00Properties.put(multipleFloat, multiDec);
        content00Properties.put(singleDouble, one);
        content00Properties.put(multipleDouble, multiDec);
        MultiPropertyValue multiInt = new MultiPropertyValue();
        multiInt.addValue(one);
        multiInt.addValue(two);
        content00Properties.put(singleInteger, one);
        content00Properties.put(multipleInteger, multiInt);
        content00Properties.put(singleLong, one);
        content00Properties.put(multipleLong, multiInt);

        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date1);
        cal.add(Calendar.DAY_OF_MONTH, -1);
        Date date0 = cal.getTime();
        cal.add(Calendar.DAY_OF_MONTH, 2);
        Date date2 = cal.getTime();
        StringPropertyValue d0 = new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date0));
        StringPropertyValue d1 = new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date1));
        StringPropertyValue d2 = new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, date2));
        MultiPropertyValue multiDate = new MultiPropertyValue();
        multiDate.addValue(d1);
        multiDate.addValue(d2);
        content00Properties.put(singleDate, d1);
        content00Properties.put(multipleDate, multiDate);
        content00Properties.put(singleDatetime, d1);
        content00Properties.put(multipleDatetime, multiDate);

        StringPropertyValue bTrue = new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, true));
        StringPropertyValue bFalse = new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, false));
        MultiPropertyValue multiBool = new MultiPropertyValue();
        multiBool.addValue(bTrue);
        multiBool.addValue(bFalse);

        content00Properties.put(singleBoolean, bTrue);
        content00Properties.put(multipleBoolean, multiBool);

        NodeRef content00NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName content00QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "Test One");
        ChildAssociationRef content00CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, folder00NodeRef,
                content00QName, content00NodeRef, true, 0);
        addNode(h.getCore(),
                dataModel,
                1,
                100,
                1,
                extendedContent,
                new QName[] { ContentModel.ASPECT_OWNABLE, ContentModel.ASPECT_TITLED },
                content00Properties,
                null,
                "andy",
                new ChildAssociationRef[] { content00CAR },
                new NodeRef[] { baseFolderNodeRef, rootNodeRef, folder00NodeRef },
                new String[] { "/" + baseFolderQName.toString() + "/" + folder00QName.toString() + "/" + content00QName.toString() },
                content00NodeRef,
                true);
    }


    public static void loadTestSet() throws IOException {
        // Root
        SolrCore core = h.getCore();
        AlfrescoSolrDataModel dataModel = AlfrescoSolrDataModel.getInstance();
        dataModel.setCMDefaultUri();

        NodeRef rootNodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        testRootNodeRef = rootNodeRef;
        addStoreRoot(core, dataModel, rootNodeRef, 1, 1, 1, 1);

        // 1

        NodeRef n01NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());

        testNodeRef = n01NodeRef;

        QName n01QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "one");
        ChildAssociationRef n01CAR = new ChildAssociationRef(ContentModel.ASSOC_CHILDREN, rootNodeRef, n01QName,
                n01NodeRef, true, 0);
        addNode(core, dataModel, 1, 2, 1, testSuperType, null, getOrderProperties(), null, "andy",
                new ChildAssociationRef[] { n01CAR }, new NodeRef[] { rootNodeRef }, new String[] { "/"
                        + n01QName.toString() }, n01NodeRef, true);

        // 2

        NodeRef n02NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n02QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "two");
        ChildAssociationRef n02CAR = new ChildAssociationRef(ContentModel.ASSOC_CHILDREN, rootNodeRef, n02QName,
                n02NodeRef, true, 0);
        addNode(core, dataModel, 1, 3, 1, testSuperType, null, getOrderProperties(), null, "bob",
                new ChildAssociationRef[] { n02CAR }, new NodeRef[] { rootNodeRef }, new String[] { "/"
                        + n02QName.toString() }, n02NodeRef, true);

        // 3

        NodeRef n03NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n03QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "three");
        ChildAssociationRef n03CAR = new ChildAssociationRef(ContentModel.ASSOC_CHILDREN, rootNodeRef, n03QName,
                n03NodeRef, true, 0);
        addNode(core, dataModel, 1, 4, 1, testSuperType, null, getOrderProperties(), null, "cid",
                new ChildAssociationRef[] { n03CAR }, new NodeRef[] { rootNodeRef }, new String[] { "/"
                        + n03QName.toString() }, n03NodeRef, true);

        // 4

        HashMap<QName, PropertyValue> properties04 = new HashMap<QName, PropertyValue>();
        HashMap<QName, String> content04 = new HashMap<QName, String>();
        properties04.putAll(getOrderProperties());
        properties04.put(QName.createQName(TEST_NAMESPACE, "text-indexed-stored-tokenised-atomic"),
                new StringPropertyValue("TEXT THAT IS INDEXED STORED AND TOKENISED ATOMICALLY KEYONE"));
        properties04.put(QName.createQName(TEST_NAMESPACE, "text-indexed-unstored-tokenised-atomic"),
                new StringPropertyValue("TEXT THAT IS INDEXED STORED AND TOKENISED ATOMICALLY KEYUNSTORED"));
        properties04.put(QName.createQName(TEST_NAMESPACE, "text-indexed-stored-tokenised-nonatomic"),
                new StringPropertyValue("TEXT THAT IS INDEXED STORED AND TOKENISED BUT NOT ATOMICALLY KEYTWO"));
        properties04.put(QName.createQName(TEST_NAMESPACE, "int-ista"), new StringPropertyValue("1"));
        properties04.put(QName.createQName(TEST_NAMESPACE, "long-ista"), new StringPropertyValue("2"));
        properties04.put(QName.createQName(TEST_NAMESPACE, "float-ista"), new StringPropertyValue("3.4"));
        properties04.put(QName.createQName(TEST_NAMESPACE, "double-ista"), new StringPropertyValue("5.6"));

        Calendar c = new GregorianCalendar();
        c.setTime(new Date(((new Date().getTime() - 10000))));
        Date testDate = c.getTime();
        properties04.put(QName.createQName(TEST_NAMESPACE, "date-ista"), new StringPropertyValue(
                DefaultTypeConverter.INSTANCE.convert(String.class, testDate)));
        properties04.put(QName.createQName(TEST_NAMESPACE, "datetime-ista"), new StringPropertyValue(
                DefaultTypeConverter.INSTANCE.convert(String.class, testDate)));
        properties04.put(QName.createQName(TEST_NAMESPACE, "boolean-ista"), new StringPropertyValue(
                DefaultTypeConverter.INSTANCE.convert(String.class, Boolean.valueOf(true))));
        properties04.put(QName.createQName(TEST_NAMESPACE, "qname-ista"), new StringPropertyValue(
                DefaultTypeConverter.INSTANCE.convert(String.class, QName.createQName("{wibble}wobble"))));
        properties04.put(
                QName.createQName(TEST_NAMESPACE, "category-ista"),
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, new NodeRef(
                        new StoreRef("proto", "id"), "CategoryId"))));
        properties04.put(QName.createQName(TEST_NAMESPACE, "noderef-ista"), new StringPropertyValue(
                DefaultTypeConverter.INSTANCE.convert(String.class, n01NodeRef)));
        properties04.put(QName.createQName(TEST_NAMESPACE, "path-ista"),
                new StringPropertyValue("/" + n03QName.toString()));
        properties04.put(QName.createQName(TEST_NAMESPACE, "locale-ista"), new StringPropertyValue(
                DefaultTypeConverter.INSTANCE.convert(String.class, Locale.UK)));
        properties04.put(QName.createQName(TEST_NAMESPACE, "period-ista"), new StringPropertyValue(
                DefaultTypeConverter.INSTANCE.convert(String.class, new Period("period|12"))));
        properties04.put(QName.createQName(TEST_NAMESPACE, "null"), null);
        MultiPropertyValue list_0 = new MultiPropertyValue();
        list_0.addValue(new StringPropertyValue("one"));
        list_0.addValue(new StringPropertyValue("two"));
        properties04.put(QName.createQName(TEST_NAMESPACE, "list"), list_0);
        MLTextPropertyValue mlText = new MLTextPropertyValue();
        mlText.addValue(Locale.ENGLISH, "banana");
        mlText.addValue(Locale.FRENCH, "banane");
        mlText.addValue(Locale.CHINESE, "香蕉");
        mlText.addValue(new Locale("nl"), "banaan");
        mlText.addValue(Locale.GERMAN, "banane");
        mlText.addValue(new Locale("el"), "μπανάνα");
        mlText.addValue(Locale.ITALIAN, "banana");
        mlText.addValue(new Locale("ja"), "�?ナナ");
        mlText.addValue(new Locale("ko"), "바나나");
        mlText.addValue(new Locale("pt"), "banana");
        mlText.addValue(new Locale("ru"), "банан");
        mlText.addValue(new Locale("es"), "plátano");
        properties04.put(QName.createQName(TEST_NAMESPACE, "ml"), mlText);
        MultiPropertyValue list_1 = new MultiPropertyValue();
        list_1.addValue(new StringPropertyValue("100"));
        list_1.addValue(new StringPropertyValue("anyValueAsString"));
        properties04.put(QName.createQName(TEST_NAMESPACE, "any-many-ista"), list_1);
        MultiPropertyValue list_2 = new MultiPropertyValue();
        list_2.addValue(new ContentPropertyValue(Locale.ENGLISH, 12L, "UTF-16", "text/plain", null));
        properties04.put(QName.createQName(TEST_NAMESPACE, "content-many-ista"), list_2);
        content04.put(QName.createQName(TEST_NAMESPACE, "content-many-ista"), "multicontent");

        MLTextPropertyValue mlText1 = new MLTextPropertyValue();
        mlText1.addValue(Locale.ENGLISH, "cabbage");
        mlText1.addValue(Locale.FRENCH, "chou");

        MLTextPropertyValue mlText2 = new MLTextPropertyValue();
        mlText2.addValue(Locale.ENGLISH, "lemur");
        mlText2.addValue(new Locale("ru"), "лемур");

        MultiPropertyValue list_3 = new MultiPropertyValue();
        list_3.addValue(mlText1);
        list_3.addValue(mlText2);

        properties04.put(QName.createQName(TEST_NAMESPACE, "mltext-many-ista"), list_3);

        MultiPropertyValue list_4 = new MultiPropertyValue();
        list_4.addValue(null);
        properties04.put(QName.createQName(TEST_NAMESPACE, "nullist"), list_4);

        NodeRef n04NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n04QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "four");
        ChildAssociationRef n04CAR = new ChildAssociationRef(ContentModel.ASSOC_CHILDREN, rootNodeRef, n04QName,
                n04NodeRef, true, 0);

        properties04.put(QName.createQName(TEST_NAMESPACE, "aspectProperty"), new StringPropertyValue(""));
        addNode(core, dataModel, 1, 5, 1, testType, new QName[] { testAspect }, properties04, content04, "dave",
                new ChildAssociationRef[] { n04CAR }, new NodeRef[] { rootNodeRef }, new String[] { "/"
                        + n04QName.toString() }, n04NodeRef, true);

        // 5

        NodeRef n05NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n05QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "five");
        ChildAssociationRef n05CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n01NodeRef, n05QName,
                n05NodeRef, true, 0);
        addNode(core, dataModel, 1, 6, 1, testSuperType, null, getOrderProperties(), null, "eoin",
                new ChildAssociationRef[] { n05CAR }, new NodeRef[] { rootNodeRef, n01NodeRef },
                new String[] { "/" + n01QName.toString() + "/" + n05QName.toString() }, n05NodeRef, true);

        // 6

        NodeRef n06NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n06QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "six");
        ChildAssociationRef n06CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n01NodeRef, n06QName,
                n06NodeRef, true, 0);
        addNode(core, dataModel, 1, 7, 1, testSuperType, null, getOrderProperties(), null, "fred",
                new ChildAssociationRef[] { n06CAR }, new NodeRef[] { rootNodeRef, n01NodeRef },
                new String[] { "/" + n01QName.toString() + "/" + n06QName.toString() }, n06NodeRef, true);

        // 7

        NodeRef n07NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n07QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "seven");
        ChildAssociationRef n07CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n02NodeRef, n07QName,
                n07NodeRef, true, 0);
        addNode(core, dataModel, 1, 8, 1, testSuperType, null, getOrderProperties(), null, "gail",
                new ChildAssociationRef[] { n07CAR }, new NodeRef[] { rootNodeRef, n02NodeRef },
                new String[] { "/" + n02QName.toString() + "/" + n07QName.toString() }, n07NodeRef, true);

        // 8

        NodeRef n08NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n08QName_0 = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "eight-0");
        QName n08QName_1 = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "eight-1");
        QName n08QName_2 = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "eight-2");
        ChildAssociationRef n08CAR_0 = new ChildAssociationRef(ContentModel.ASSOC_CHILDREN, rootNodeRef,
                n08QName_0, n08NodeRef, false, 2);
        ChildAssociationRef n08CAR_1 = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n01NodeRef, n08QName_1,
                n08NodeRef, false, 1);
        ChildAssociationRef n08CAR_2 = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n02NodeRef, n08QName_2,
                n08NodeRef, true, 0);

        addNode(core, dataModel, 1, 9, 1, testSuperType, null, getOrderProperties(), null, "hal",
                new ChildAssociationRef[] { n08CAR_0, n08CAR_1, n08CAR_2 }, new NodeRef[] { rootNodeRef,
                        rootNodeRef, n01NodeRef, rootNodeRef, n02NodeRef }, new String[] {
                        "/" + n08QName_0, "/" + n01QName.toString() + "/" + n08QName_1.toString(),
                        "/" + n02QName.toString() + "/" + n08QName_2.toString() }, n08NodeRef, true);

        // 9

        NodeRef n09NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n09QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "nine");
        ChildAssociationRef n09CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n05NodeRef, n09QName,
                n09NodeRef, true, 0);
        addNode(core, dataModel, 1, 10, 1, testSuperType, null, getOrderProperties(), null, "ian",
                new ChildAssociationRef[] { n09CAR }, new NodeRef[] { rootNodeRef, n01NodeRef, n05NodeRef },
                new String[] { "/" + n01QName.toString() + "/" + n05QName.toString() + "/" + n09QName },
                n09NodeRef, true);

        // 10

        NodeRef n10NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n10QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "ten");
        ChildAssociationRef n10CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n05NodeRef, n10QName,
                n10NodeRef, true, 0);
        addNode(core, dataModel, 1, 11, 1, testSuperType, null, getOrderProperties(), null, "jake",
                new ChildAssociationRef[] { n10CAR }, new NodeRef[] { rootNodeRef, n01NodeRef, n05NodeRef },
                new String[] { "/" + n01QName.toString() + "/" + n05QName.toString() + "/" + n10QName },
                n10NodeRef, true);

        // 11

        NodeRef n11NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n11QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "eleven");
        ChildAssociationRef n11CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n05NodeRef, n11QName,
                n11NodeRef, true, 0);
        addNode(core, dataModel, 1, 12, 1, testSuperType, null, getOrderProperties(), null, "kara",
                new ChildAssociationRef[] { n11CAR }, new NodeRef[] { rootNodeRef, n01NodeRef, n05NodeRef },
                new String[] { "/" + n01QName.toString() + "/" + n05QName.toString() + "/" + n11QName },
                n11NodeRef, true);

        // 12

        NodeRef n12NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n12QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "twelve");
        ChildAssociationRef n12CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n05NodeRef, n12QName,
                n12NodeRef, true, 0);
        addNode(core, dataModel, 1, 13, 1, testSuperType, null, getOrderProperties(), null, "loon",
                new ChildAssociationRef[] { n12CAR }, new NodeRef[] { rootNodeRef, n01NodeRef, n05NodeRef },
                new String[] { "/" + n01QName.toString() + "/" + n05QName.toString() + "/" + n12QName },
                n12NodeRef, true);

        // 13

        NodeRef n13NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n13QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "thirteen");
        QName n13QNameLink = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "link");
        ChildAssociationRef n13CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n12NodeRef, n13QName,
                n13NodeRef, true, 0);
        ChildAssociationRef n13CARLink = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n02NodeRef, n13QName,
                n13NodeRef, false, 0);
        addNode(core, dataModel, 1, 14, 1, testSuperType, null, getOrderProperties(), null, "mike",
                new ChildAssociationRef[] { n13CAR, n13CARLink }, new NodeRef[] { rootNodeRef, n01NodeRef,
                        n05NodeRef, n12NodeRef, rootNodeRef, n02NodeRef },
                new String[] {
                        "/" + n01QName.toString() + "/" + n05QName.toString() + "/" + n12QName + "/"
                                + n13QName, "/" + n02QName.toString() + "/" + n13QNameLink },
                n13NodeRef, true);

        // 14

        HashMap<QName, PropertyValue> properties14 = new HashMap<QName, PropertyValue>();
        properties14.putAll(getOrderProperties());
        HashMap<QName, String> content14 = new HashMap<QName, String>();
        MLTextPropertyValue desc1 = new MLTextPropertyValue();
        desc1.addValue(Locale.ENGLISH, "Alfresco tutorial");
        desc1.addValue(Locale.US, "Alfresco tutorial");

        Date explicitCreatedDate = new Date();
        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        properties14.put(ContentModel.PROP_CONTENT,
                new ContentPropertyValue(Locale.UK, 298L, "UTF-8", "text/plain", null));
        content14.put(
                ContentModel.PROP_CONTENT,
                "The quick brown fox jumped over the lazy dog and ate the Alfresco Tutorial, in pdf format, along with the following stop words;  a an and are"
                        + " as at be but by for if in into is it no not of on or such that the their then there these they this to was will with: "
                        + " and random charcters \u00E0\u00EA\u00EE\u00F0\u00F1\u00F6\u00FB\u00FF");
        properties14.put(ContentModel.PROP_DESCRIPTION, desc1);
        properties14.put(
                ContentModel.PROP_CREATED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE
                        .convert(String.class, explicitCreatedDate)));
        properties14.put(
                ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE
                        .convert(String.class, explicitCreatedDate)));
        MLTextPropertyValue title = new MLTextPropertyValue();
        title.addValue(Locale.ENGLISH, "English123");
        title.addValue(Locale.FRENCH, "French123");
        properties14.put(ContentModel.PROP_TITLE, title);
        NodeRef n14NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n14QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "fourteen");
        QName n14QNameCommon = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "common");
        ChildAssociationRef n14CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n13NodeRef, n14QName,
                n14NodeRef, true, 0);
        ChildAssociationRef n14CAR_1 = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n01NodeRef,
                n14QNameCommon, n14NodeRef, false, 0);
        ChildAssociationRef n14CAR_2 = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n02NodeRef,
                n14QNameCommon, n14NodeRef, false, 0);
        ChildAssociationRef n14CAR_5 = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n05NodeRef,
                n14QNameCommon, n14NodeRef, false, 0);
        ChildAssociationRef n14CAR_6 = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n06NodeRef,
                n14QNameCommon, n14NodeRef, false, 0);
        ChildAssociationRef n14CAR_12 = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n12NodeRef,
                n14QNameCommon, n14NodeRef, false, 0);
        ChildAssociationRef n14CAR_13 = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n13NodeRef,
                n14QNameCommon, n14NodeRef, false, 0);
        addNode(core, dataModel, 1, 15, 1, ContentModel.TYPE_CONTENT, new QName[] {ContentModel.ASPECT_TITLED }, properties14, content14, "noodle",
                new ChildAssociationRef[] { n14CAR, n14CAR_1, n14CAR_2, n14CAR_5, n14CAR_6, n14CAR_12,
                        n14CAR_13 }, new NodeRef[] { rootNodeRef, n01NodeRef, n05NodeRef, n12NodeRef,
                        n13NodeRef }, new String[] {
                        "/" + n01QName.toString() + "/" + n05QName.toString() + "/" + n12QName + "/"
                                + n13QName + "/" + n14QName,
                        "/" + n02QName.toString() + "/" + n13QNameLink + "/" + n14QName,
                        "/" + n01QName + "/" + n14QNameCommon,
                        "/" + n02QName + "/" + n14QNameCommon,
                        "/" + n01QName + "/" + n05QName + "/" + n14QNameCommon,
                        "/" + n01QName + "/" + n06QName + "/" + n14QNameCommon,
                        "/" + n01QName + "/" + n05QName + "/" + n12QName + "/" + n14QNameCommon,
                        "/" + n01QName + "/" + n05QName + "/" + n12QName + "/" + n13QName + "/"
                                + n14QNameCommon }, n14NodeRef, true);

        // 15

        HashMap<QName, PropertyValue> properties15 = new HashMap<QName, PropertyValue>();
        properties15.putAll(getOrderProperties());
        properties15.put(
                ContentModel.PROP_MODIFIED,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE
                        .convert(String.class, explicitCreatedDate)));
        HashMap<QName, String> content15 = new HashMap<QName, String>();
        content15.put(ContentModel.PROP_CONTENT, "          ");
        NodeRef n15NodeRef = new NodeRef(new StoreRef("workspace", "SpacesStore"), createGUID());
        QName n15QName = QName.createQName(NamespaceService.CONTENT_MODEL_1_0_URI, "fifteen");
        ChildAssociationRef n15CAR = new ChildAssociationRef(ContentModel.ASSOC_CONTAINS, n13NodeRef, n15QName,
                n15NodeRef, true, 0);
        addNode(core, dataModel, 1, 16, 1, ContentModel.TYPE_THUMBNAIL, null, properties15, content15, "ood",
                new ChildAssociationRef[] { n15CAR }, new NodeRef[] { rootNodeRef, n01NodeRef, n05NodeRef,
                        n12NodeRef, n13NodeRef },
                new String[] {
                        "/" + n01QName.toString() + "/" + n05QName.toString() + "/" + n12QName + "/"
                                + n13QName + "/" + n15QName,
                        "/" + n02QName.toString() + "/" + n13QNameLink + "/" + n14QName }, n15NodeRef, true);






    }

    private static SolrInputDocument createDocument(AlfrescoSolrDataModel dataModel, Long txid, Long dbid, NodeRef nodeRef,
                                             QName type, QName[] aspects, Map<QName, PropertyValue> properties, Map<QName, String> content,
                                             Long aclId, String[] paths, String owner, ChildAssociationRef[] parentAssocs, NodeRef[] ancestors)
            throws IOException
    {
        SolrInputDocument doc = new SolrInputDocument();
        String id = AlfrescoSolrDataModel.getNodeDocumentId(AlfrescoSolrDataModel.DEFAULT_TENANT, aclId, dbid);
        doc.addField(FIELD_SOLR4_ID, id);
        doc.addField(FIELD_VERSION, 0);
        doc.addField(FIELD_DBID, "" + dbid);
        doc.addField(FIELD_LID, nodeRef);
        doc.addField(FIELD_INTXID, "" + txid);
        doc.addField(FIELD_ACLID, "" + aclId);
        doc.addField(FIELD_DOC_TYPE, SolrInformationServer.DOC_TYPE_NODE);

        if (paths != null)
        {
            for (String path : paths)
            {
                doc.addField(FIELD_PATH, path);
            }
        }

        if (owner != null)
        {
            doc.addField(FIELD_OWNER, owner);
        }
        doc.addField(FIELD_PARENT_ASSOC_CRC, "0");

        StringBuilder qNameBuffer = new StringBuilder(64);
        StringBuilder assocTypeQNameBuffer = new StringBuilder(64);
        if (parentAssocs != null)
        {
            for (ChildAssociationRef childAssocRef : parentAssocs)
            {
                if (qNameBuffer.length() > 0)
                {
                    qNameBuffer.append(";/");
                    assocTypeQNameBuffer.append(";/");
                }
                qNameBuffer.append(ISO9075.getXPathName(childAssocRef.getQName()));
                assocTypeQNameBuffer.append(ISO9075.getXPathName(childAssocRef.getTypeQName()));
                doc.addField(FIELD_PARENT, childAssocRef.getParentRef());

                if (childAssocRef.isPrimary())
                {
                    doc.addField(FIELD_PRIMARYPARENT, childAssocRef.getParentRef());
                    doc.addField(FIELD_PRIMARYASSOCTYPEQNAME,
                            ISO9075.getXPathName(childAssocRef.getTypeQName()));
                    doc.addField(FIELD_PRIMARYASSOCQNAME, ISO9075.getXPathName(childAssocRef.getQName()));
                }
            }
            doc.addField(FIELD_ASSOCTYPEQNAME, assocTypeQNameBuffer.toString());
            doc.addField(FIELD_QNAME, qNameBuffer.toString());
        }

        if (ancestors != null)
        {
            for (NodeRef ancestor : ancestors)
            {
                doc.addField(FIELD_ANCESTOR, ancestor.toString());
            }
        }

        if (properties != null)
        {
            final boolean isContentIndexedForNode = true;
            final SolrInputDocument cachedDoc = null;
            final boolean transformContentFlag = true;
            SolrInformationServer.addPropertiesToDoc(properties, isContentIndexedForNode, doc, cachedDoc, transformContentFlag);
            addContentToDoc(doc, content);
        }

        doc.addField(FIELD_TYPE, type);
        if (aspects != null)
        {
            for (QName aspect : aspects)
            {
                doc.addField(FIELD_ASPECT, aspect);
            }
        }
        doc.addField(FIELD_ISNODE, "T");
        doc.addField(FIELD_TENANT, AlfrescoSolrDataModel.DEFAULT_TENANT);

        return doc;
    }

    private static void addContentToDoc(SolrInputDocument cachedDoc, Map<QName, String> content)
    {
        Collection<String> fieldNames = cachedDoc.deepCopy().getFieldNames();
        for (String fieldName : fieldNames)
        {
            if (fieldName.startsWith(AlfrescoSolrDataModel.CONTENT_S_LOCALE_PREFIX))
            {
                String locale = String.valueOf(cachedDoc.getFieldValue(fieldName));
                String qNamePart = fieldName.substring(AlfrescoSolrDataModel.CONTENT_S_LOCALE_PREFIX.length());
                QName propertyQName = QName.createQName(qNamePart);
                addContentPropertyToDoc(cachedDoc, propertyQName, locale, content);
            }
            // Could update multi content but it is broken ....
        }
    }

    private static void addContentPropertyToDoc(SolrInputDocument cachedDoc,
                                         QName propertyQName,
                                         String locale,
                                         Map<QName, String> content)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("\u0000").append(locale).append("\u0000");
        builder.append(content.get(propertyQName));

        for (AlfrescoSolrDataModel.FieldInstance field : AlfrescoSolrDataModel.getInstance().getIndexedFieldNamesForProperty(propertyQName).getFields())
        {
            cachedDoc.removeField(field.getField());
            if(field.isLocalised())
            {
                cachedDoc.addField(field.getField(), builder.toString());
            }
            else
            {
                cachedDoc.addField(field.getField(), content.get(propertyQName));
            }
        }
    }

    private static NodeRef addNode(SolrCore core, AlfrescoSolrDataModel dataModel, int txid, int dbid, int aclid, QName type,
                            QName[] aspects, Map<QName, PropertyValue> properties, Map<QName, String> content, String owner,
                            ChildAssociationRef[] parentAssocs, NodeRef[] ancestors, String[] paths, NodeRef nodeRef, boolean commit)
            throws IOException
    {
        SolrServletRequest solrQueryRequest = null;

        try
        {
            solrQueryRequest = new SolrServletRequest(core, null);
            AddUpdateCommand addDocCmd = new AddUpdateCommand(solrQueryRequest);
            addDocCmd.overwrite = true;
            addDocCmd.solrDoc = createDocument(dataModel, new Long(txid), new Long(dbid), nodeRef, type, aspects,
                    properties, content, new Long(aclid), paths, owner, parentAssocs, ancestors);
            core.getUpdateHandler().addDoc(addDocCmd);

            if (commit)
            {
                core.getUpdateHandler().commit(new CommitUpdateCommand(solrQueryRequest, false));
            }

        }
        finally
        {
            solrQueryRequest.close();
        }
        return nodeRef;
    }


    private static void addStoreRoot(SolrCore core,
                                     AlfrescoSolrDataModel dataModel,
                                     NodeRef rootNodeRef,
                                     int txid,
                                     int dbid,
                                     int acltxid,
                                     int aclid) throws IOException
    {

        SolrServletRequest solrQueryRequest = null;

        try {
            solrQueryRequest = new SolrServletRequest(core, null);
            AddUpdateCommand addDocCmd = new AddUpdateCommand(solrQueryRequest);
            addDocCmd.overwrite = true;
            addDocCmd.solrDoc = createDocument(dataModel, new Long(txid), new Long(dbid), rootNodeRef,
                    ContentModel.TYPE_STOREROOT, new QName[]{ContentModel.ASPECT_ROOT}, null, null, new Long(aclid),
                    new String[]{"/"}, "system", null, null);
            core.getUpdateHandler().addDoc(addDocCmd);
            addAcl(solrQueryRequest, core, dataModel, acltxid, aclid, 0, 0);

            AddUpdateCommand txCmd = new AddUpdateCommand(solrQueryRequest);
            txCmd.overwrite = true;
            SolrInputDocument input = new SolrInputDocument();
            String id = AlfrescoSolrDataModel.getTransactionDocumentId(new Long(txid));
            input.addField(FIELD_SOLR4_ID, id);
            input.addField(FIELD_VERSION, "0");
            input.addField(FIELD_TXID, txid);
            input.addField(FIELD_INTXID, txid);
            input.addField(FIELD_TXCOMMITTIME, (new Date()).getTime());
            input.addField(FIELD_DOC_TYPE, SolrInformationServer.DOC_TYPE_TX);
            txCmd.solrDoc = input;
            core.getUpdateHandler().addDoc(txCmd);

            core.getUpdateHandler().commit(new CommitUpdateCommand(solrQueryRequest, false));
        } finally {
            solrQueryRequest.close();
        }
    }

    private static void addAcl(SolrServletRequest solrQueryRequest, SolrCore core, AlfrescoSolrDataModel dataModel, int acltxid, int aclId, int maxReader,
                        int totalReader) throws IOException
    {
        AddUpdateCommand aclTxCmd = new AddUpdateCommand(solrQueryRequest);
        aclTxCmd.overwrite = true;
        SolrInputDocument aclTxSol = new SolrInputDocument();
        String aclTxId = AlfrescoSolrDataModel.getAclChangeSetDocumentId(new Long(acltxid));
        aclTxSol.addField(FIELD_SOLR4_ID, aclTxId);
        aclTxSol.addField(FIELD_VERSION, "0");
        aclTxSol.addField(FIELD_ACLTXID, acltxid);
        aclTxSol.addField(FIELD_INACLTXID, acltxid);
        aclTxSol.addField(FIELD_ACLTXCOMMITTIME, (new Date()).getTime());
        aclTxSol.addField(FIELD_DOC_TYPE, SolrInformationServer.DOC_TYPE_ACL_TX);
        aclTxCmd.solrDoc = aclTxSol;
        core.getUpdateHandler().addDoc(aclTxCmd);

        AddUpdateCommand aclCmd = new AddUpdateCommand(solrQueryRequest);
        aclCmd.overwrite = true;
        SolrInputDocument aclSol = new SolrInputDocument();
        String aclDocId = AlfrescoSolrDataModel.getAclDocumentId(AlfrescoSolrDataModel.DEFAULT_TENANT, new Long(aclId));
        aclSol.addField(FIELD_SOLR4_ID, aclDocId);
        aclSol.addField(FIELD_VERSION, "0");
        aclSol.addField(FIELD_ACLID, aclId);
        aclSol.addField(FIELD_INACLTXID, "" + acltxid);
        aclSol.addField(FIELD_READER, "GROUP_EVERYONE");
        aclSol.addField(FIELD_READER, "pig");
        for (int i = 0; i <= maxReader; i++)
        {
            aclSol.addField(FIELD_READER, "READER-" + (totalReader - i));
        }
        aclSol.addField(FIELD_DENIED, "something");
        aclSol.addField(FIELD_DOC_TYPE, SolrInformationServer.DOC_TYPE_ACL);
        aclCmd.solrDoc = aclSol;
        core.getUpdateHandler().addDoc(aclCmd);
    }

    private static  Map<QName, PropertyValue> getOrderProperties()
    {
        double orderDoubleCount = -0.11d + orderTextCount * ((orderTextCount % 2 == 0) ? 0.1d : -0.1d);
        float orderFloatCount = -3.5556f + orderTextCount * ((orderTextCount % 2 == 0) ? 0.82f : -0.82f);
        long orderLongCount = -1999999999999999l + orderTextCount
                * ((orderTextCount % 2 == 0) ? 299999999999999l : -299999999999999l);
        int orderIntCount = -45764576 + orderTextCount * ((orderTextCount % 2 == 0) ? 8576457 : -8576457);

        Map<QName, PropertyValue> testProperties = new HashMap<QName, PropertyValue>();
        testProperties.put(createdDate,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, orderDate)));
        testProperties.put(createdTime,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, orderDate)));
        testProperties.put(orderDouble,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, orderDoubleCount)));
        testProperties.put(orderFloat,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, orderFloatCount)));
        testProperties.put(orderLong,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, orderLongCount)));
        testProperties.put(orderInt,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, orderIntCount)));
        testProperties.put(
                orderText,
                new StringPropertyValue(DefaultTypeConverter.INSTANCE.convert(String.class, new String(
                        new char[] { (char) ('l' + ((orderTextCount % 2 == 0) ? orderTextCount
                                : -orderTextCount)) })
                        + " cabbage")));

        testProperties.put(ContentModel.PROP_NAME, new StringPropertyValue(orderNames[orderTextCount]));
        testProperties.put(orderLocalisedText, new StringPropertyValue(orderLocalisedNames[orderTextCount]));

        MLTextPropertyValue mlTextPropLocalisedOrder = new MLTextPropertyValue();
        if (orderLocaliseMLText_en[orderTextCount].length() > 0)
        {
            mlTextPropLocalisedOrder.addValue(Locale.ENGLISH, orderLocaliseMLText_en[orderTextCount]);
        }
        if (orderLocaliseMLText_fr[orderTextCount].length() > 0)
        {
            mlTextPropLocalisedOrder.addValue(Locale.FRENCH, orderLocaliseMLText_fr[orderTextCount]);
        }
        if (orderLocaliseMLText_es[orderTextCount].length() > 0)
        {
            mlTextPropLocalisedOrder.addValue(new Locale("es"), orderLocaliseMLText_es[orderTextCount]);
        }
        if (orderLocaliseMLText_de[orderTextCount].length() > 0)
        {
            mlTextPropLocalisedOrder.addValue(Locale.GERMAN, orderLocaliseMLText_de[orderTextCount]);
        }
        testProperties.put(orderLocalisedMLText, mlTextPropLocalisedOrder);

        MLTextPropertyValue mlTextPropVal = new MLTextPropertyValue();
        mlTextPropVal.addValue(Locale.ENGLISH, new String(
                new char[]{(char) ('l' + ((orderTextCount % 2 == 0) ? orderTextCount : -orderTextCount))})
                + " banana");
        mlTextPropVal.addValue(Locale.FRENCH, new String(
                new char[]{(char) ('L' + ((orderTextCount % 2 == 0) ? -orderTextCount : orderTextCount))})
                + " banane");
        mlTextPropVal.addValue(Locale.CHINESE, new String(
                new char[]{(char) ('香' + ((orderTextCount % 2 == 0) ? orderTextCount : -orderTextCount))})
                + " 香蕉");
        testProperties.put(orderMLText, mlTextPropVal);

        orderDate = Duration.subtract(orderDate, new Duration("P1D"));
        orderTextCount++;
        return testProperties;
    }

    protected void assertAQuery(String queryString,
                                Integer count)
            throws IOException,
                   org.apache.lucene.queryparser.classic.ParseException {
        assertAQuery(queryString, count, null, null, null);
    }


    protected void assertAQuery(String queryString,
                                Integer count,
                                Locale locale,
                                String[] textAttributes,
                                String[] allAttributes,
                                String... name)
            throws IOException,
                   org.apache.lucene.queryparser.classic.ParseException
    {
        SolrServletRequest solrQueryRequest = null;
        RefCounted<SolrIndexSearcher>refCounted = null;
        try
        {
            solrQueryRequest = new SolrServletRequest(h.getCore(), null);
            refCounted = h.getCore().getSearcher();
            SolrIndexSearcher solrIndexSearcher = refCounted.get();
            SearchParameters searchParameters = new SearchParameters();
            searchParameters.setQuery(queryString);

            if (locale != null)
            {
                searchParameters.addLocale(locale);
            }

            if (textAttributes != null)
            {
                for (String textAttribute : textAttributes)
                {
                    searchParameters.addTextAttribute(textAttribute);
                }
            }

            if (allAttributes != null)
            {
                for (String allAttribute : allAttributes)
                {
                    searchParameters.addAllAttribute(allAttribute);
                }
            }

            Query query = dataModel.getLuceneQueryParser(searchParameters, solrQueryRequest, FTSQueryParser.RerankPhase.SINGLE_PASS).parse(queryString);
            System.out.println("####### Query ######:" + query);
            TopDocs docs = solrIndexSearcher.search(query, count * 2 + 10);

            if (count != null)
            {
                if (docs.totalHits != count)
                {
                    throw new IOException("FAILED: " + fixQueryString(queryString, name)+" ; "+docs.totalHits);
                }
            }
        }
        finally
        {
            refCounted.decref();
            solrQueryRequest.close();
        }
    }

    private String fixQueryString(String queryString, String... name)
    {
        if (name.length > 0)
        {
            return name[0].replace("\uFFFF", "<Unicode FFFF>");
        }
        else
        {
            return queryString.replace("\uFFFF", "<Unicode FFFF>");
        }
    }

    protected static MLTextPropertyValue makeMLText()
    {
        return makeMLText(0);
    }

    protected static MLTextPropertyValue makeMLText(int position)
    {
        MLTextPropertyValue ml = new MLTextPropertyValue();
        ml.addValue(Locale.ENGLISH, mlOrderable_en[position]);
        ml.addValue(Locale.FRENCH, mlOrderable_fr[position]);
        return ml;
    }

    protected static MultiPropertyValue makeMLTextMVP()
    {
        return makeMLTextMVP(0);
    }

    protected static MultiPropertyValue makeMLTextMVP(int position)
    {
        MLTextPropertyValue m1 = new MLTextPropertyValue();
        m1.addValue(Locale.ENGLISH, mlOrderable_en[position]);
        MLTextPropertyValue m2 = new MLTextPropertyValue();
        m2.addValue(Locale.FRENCH, mlOrderable_fr[position]);
        MultiPropertyValue answer = new MultiPropertyValue();
        answer.addValue(m1);
        answer.addValue(m2);
        return answer;
    }

    private static String[] mlOrderable_en = new String[] { "AAAA BBBB", "EEEE FFFF", "II", "KK", "MM", "OO", "QQ",
            "SS", "UU", "AA", "CC" };

    private static String[] mlOrderable_fr = new String[] { "CCCC DDDD", "GGGG HHHH", "JJ", "LL", "NN", "PP", "RR",
            "TT", "VV", "BB", "DD" };

}