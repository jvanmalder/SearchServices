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
package org.alfresco.solr.sql;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import org.alfresco.service.namespace.NamespaceException;
import org.alfresco.service.namespace.QName;
import org.alfresco.solr.AlfrescoSolrDataModel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.commons.collections.keyvalue.DefaultMapEntry;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.AlfrescoSQLHandler;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
* The SolrSchema class creates the "alfresco" table and populates the queryFields from the index.
*/
public class SolrSchema extends AbstractSchema
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrSchema.class);

    final Properties properties;
    final SolrCore core;

    /**
     * When the query contains a date/datetime field (e.g. "cm_created") followed by one of these postfixes (i.e. "cm_created_day")
     * the insight engine manages them in a special way, creating a "virtual" field ("cm_created_day") containing only the requested portion
     * of the date/datetime. Note that the virtual field value is no longer a date, but a string (e.g. "31", "2").
     *
     *
     * SEARCH-1315 would introduce a more SQL-compliant way to manage this, as the day/month/year extraction is part of
     * the date functions supported by Calcite (e.g. "YEAR(date)", "EXTRACT(YEAR from date)"
     *
     * @see #addTimeFields(RelDataTypeFactory.FieldInfoBuilder, Entry, RelDataType)
     */
    final String[] postfixes = { "_day", "_month", "_year" };

    final boolean isSelectStarQuery;
    final Map<String, String> queryFields = new HashMap<>();

    SolrSchema(SolrCore core, Properties properties)
    {
        super();
        this.core = core;
        this.properties = properties;
        this.isSelectStarQuery = Boolean.parseBoolean(properties.getProperty(AlfrescoSQLHandler.IS_SELECT_STAR));

        initFieldsFromConfiguration(properties);
    }

    /**
     * Init a map of field-> type from :
     * - configurations defined in the shared properties
     * - hard coded list of select star queryFields
     * - queryFields from the sql predicates
     *
     * @param properties
     */
    private void initFieldsFromConfiguration(Properties properties)
    {
        // Get fields from configuration.
        queryFields.putAll(SolrSchemaUtil.fetchCustomFieldsFromSharedProperties());

        // Get fields from default fields.
        for (SelectStarDefaultField fieldAndType : SelectStarDefaultField.values())
        {
            queryFields.putIfAbsent(fieldAndType.getFieldName(), fieldAndType.getFieldType());
        }


        Map<String, String> modelAndIndexedFields = getIndexedFieldsInfo();
        modelAndIndexedFields.putAll(getModelFieldsInfo());

        String sql = properties.getProperty("stmt", "");

        // Get fields from solr index.
        if (!isSelectStarQuery)
        {
            queryFields.putAll(modelAndIndexedFields);
        }
        else if (predicateExists(sql))
        {
            // Create set of formatted fields. (Useful to check for duplicates)
            // SEARCH-1491: queryFields is the list of fields used later (see RelProtoDataType#getRelDataType) for
            // populating the FieldInfo which is the source where Calcite picks up fields definitions.
            // Unfortunately, the case insensitive mode (which is set by default) produces a weird behaviour when
            // the same field is in this list with a different case: the first one is retrieved, even if that doesn't
            // correspond (from case perspective) to the field as it is declared in Solr.
            // This "double" addition could happen when a field is declared in two different places (e.g. SelectStarDefaultField
            // collection and the predicate list in the query).
            // The formattedFields list uses a case insensitive comparator in order to make sure a field, regardless its case,
            // is added only once to the fields catalog.
            final Set<String> formattedFieldsInserted = queryFields.keySet().stream()
                .map(this::getFormattedFieldName)
                .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));

            // This map is used to get the right type for the properties extracted from the predicate in select * queries.
            final Map<String, String> formattedFieldsFromModelAndInsex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            formattedFieldsFromModelAndInsex.putAll(modelAndIndexedFields.entrySet().stream()
                .collect(Collectors.toMap((entry)-> getFormattedFieldName(entry.getKey()),
            (entry) -> entry.getValue())));

            SolrSchemaUtil.extractPredicates(sql).stream()
                .filter(predicateField -> !formattedFieldsInserted.contains(predicateField))
                .forEach(fieldName ->
                    {
                        String type = getFormattedFieldName(fieldName);
                        if (type != null)
                        {
                            queryFields.putIfAbsent(fieldName,
                                formattedFieldsFromModelAndInsex.get(type));
                        }
                        else
                        {
                            LOGGER.warn("Unable to resolve type for fieldName: " + fieldName);
                        }
                    });
        }
    }

    public boolean predicateExists(String sql)
    {
        return (sql != null && sql.toLowerCase().contains(" where "));
    }

    @Override
    protected Map<String, Table> getTableMap()
    {
        Map<String, Table> map = new HashMap<>();
        map.put("alfresco", new SolrTable(this, "alfresco"));
        return map;
    }

    private RelDataType resolveType(String ltype, RelDataTypeFactory typeFactory)
    {
        RelDataType type;
        switch (ltype)
        {
            case "solr.StrField":
            case "solr.TextField":
            case "org.alfresco.solr.AlfrescoFieldType":
                type = typeFactory.createJavaType(String.class);
                break;
            case "solr.TrieLongField":
                type = typeFactory.createJavaType(Long.class);
                break;
            case "solr.TrieDoubleField":
                type = typeFactory.createJavaType(Double.class);
                break;
            case "solr.TrieFloatField":
                type = typeFactory.createJavaType(Double.class);
                break;
            case "solr.TrieIntField":
                type = typeFactory.createJavaType(Long.class);
                break;
            case "java.lang.Integer":
                type = typeFactory.createJavaType(Long.class);
                break;
            case "java.lang.Float":
                type = typeFactory.createJavaType(Double.class);
                break;
            case "solr.TrieDateField":
                type = typeFactory.createJavaType(String.class);
                break;
            case "java.util.Date":
                type = typeFactory.createJavaType(String.class);
                break;
            default:
                type = typeFactory.createJavaType(String.class);
        }
        return type;
    }



    /**
     * Returns the prototype factory used for further defining the data types associated with this schema.
     * Each field in the index is associated with a {@link RelDataType} which in turns maps a Java type. That will drive
     * the field management in terms of processing and parsing.
     *
     * @see #resolveType(String, RelDataTypeFactory)
     * @see SolrTable#getRowType
     * @see Table#getRowType(RelDataTypeFactory)
     * @param collection the collection associated with this schema.
     * @return the prototype factory used for further defining the data types associated with this table/schema.
     */
    RelProtoDataType getRelDataType(String collection)
    {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();


        /*
         * Load query fields
         */
        for (Entry<String, String> fieldAndType : queryFields.entrySet())
        {
            String fieldType = fieldAndType.getValue();
            String formattedFieldName = getFormattedFieldName(fieldAndType.getKey(),null);

            addFieldInfoOriginalNameAndFormatted(fieldInfo, fieldAndType,
                resolveType(fieldAndType.getValue(), typeFactory), null, formattedFieldName);

            if (fieldType.equals("java.util.Date") || fieldType.equals("solr.TrieDateField"))
            {
                addTimeFields(fieldInfo, fieldAndType, typeFactory.createJavaType(String.class));
            }

        }

        fieldInfo.add("_query_", typeFactory.createJavaType(String.class));
        fieldInfo.add("score", typeFactory.createJavaType(Double.class));

        return RelDataTypeImpl.proto(fieldInfo.build());
    }

    /**
     * Checks if the field already exists in the virtual schema.
     * @param entry
     * @return
     */
    public static boolean lockOwnerFieldExists(String entry)
    {
        if(null != entry)
        {
            return "cm_lockOwner".contentEquals(entry)|| "cm:lockOwner".contentEquals(entry);
        }
        return false;
    }

    private void addTimeFields(RelDataTypeFactory.FieldInfoBuilder fieldInfo, Map.Entry<String, String> entry, RelDataType type)
    {
        for(String postfix : postfixes)
        {
            addFieldInfoOriginalNameAndFormatted(fieldInfo, entry, type, postfix, null);
        }
    }

    private void addFieldInfoOriginalNameAndFormatted(RelDataTypeFactory.FieldInfoBuilder fieldInfo, Entry<String, String> entry, RelDataType type, String postfix, String formattedFieldName)
    {
        if(formattedFieldName==null)
        {
            formattedFieldName = getFormattedFieldName(entry.getKey(), postfix);
        }

        fieldInfo.add(entry.getKey() + getPostfix(postfix), type).nullable(true);

        if (!formattedFieldName.contentEquals(entry.getKey() + getPostfix(postfix)))
        {
            fieldInfo.add(formattedFieldName, type).nullable(true);
        }
    }

    /**
     * Returns a formatted version of the field name in input.
     * This is a special case where there's no postfix.
     *
     * @see #getFormattedFieldName(String)
     * @param fieldName the field name we want to format.
     * @return a formatted version of the input field name (e.g. cm:version -> cm_version)
     */
    private String getFormattedFieldName(String fieldName)
    {
        return getFormattedFieldName(fieldName, null);
    }

    /**
     * Returns a formatted version of the field name in input.
     * First, fieldName is split into namespace prefix (if any) and localname.
     * Then, the two parts are concatenated using an underscore as delimiter.
     * Last, this is at the moment valid only for date fields, a prefix is added (e.g. _day, _month) only if that is
     * not null.
     *
     * @param fieldName the field name we want to format.
     * @return a formatted version of the input field name (e.g. cm:version -> cm_version)
     */
    private String getFormattedFieldName(String fieldName, String postfix)
    {
        try
        {
            String[] prefixNamespaceAndLocalName = QName.splitPrefixedQName(fieldName);
            String prefix = prefixNamespaceAndLocalName[0];
            if (prefix != null && !prefix.isEmpty())
            {
                return prefixNamespaceAndLocalName[0] + "_" + prefixNamespaceAndLocalName[1] + getPostfix(postfix);
            }
        }
        catch (NamespaceException ignore) {
            //ignore invalid qnames
        }
        return fieldName;
    }

    private String getPostfix(String postfix)
    {
        return postfix != null ? postfix : "";
    }

    /**
     * Get fields and correlated solrType from data model.
     *
     * Details:
     *  Get all the properties from the default dictionary. For each property, a queryable field is taken from dataModel.
     *  The queryable field is used to search the solrType in the Solr IndexSchema
     *
     * @return
     * @throws RuntimeException
     */
    private Map<String, String> getModelFieldsInfo() throws RuntimeException
    {
        Map<String, String> map = new HashMap<>();
        AlfrescoSolrDataModel dataModel = AlfrescoSolrDataModel.getInstance();
        RefCounted<SolrIndexSearcher> refCounted = core.getSearcher();
        try
        {
            SolrIndexSearcher searcher = refCounted.get();
            IndexSchema schema = searcher.getSchema();

            dataModel
                .getDictionaryService(null)
                .getAllProperties(null)
                .stream()
                .forEach(qname ->
                    {
                        String fieldName = qname.toString();
                        List<AlfrescoSolrDataModel.FieldInstance> fields =
                            dataModel.getQueryableFields(qname, null, AlfrescoSolrDataModel.FieldUse.SORT)
                                .getFields();

                        if (!fields.isEmpty())
                        {
                            String queryableField = fields.get(0).getField();
                            if (!queryableField.equals("_dummy_")){
                                SchemaField sfield = schema.getFieldOrNull(queryableField);
                                FieldType ftype = (sfield == null) ? null : sfield.getType();

                                if (ftype != null){
                                    try
                                    {
                                        String alfrescoPropertyFromSchemaField =
                                            dataModel.getAlfrescoPropertyFromSchemaField(queryableField);
                                        String type = ftype.getClassArg();
                                        if (isNotBlank(type)){
                                            map.put(alfrescoPropertyFromSchemaField, type);
                                        }
                                    }
                                    catch (NamespaceException ne)
                                    {
                                        //Field name may have been created but now deactivated, e.g custom model.
                                        LOGGER.warn("Unable to resolve field: " + fieldName);
                                    }
                                }
                            }
                         }
                    });
        }
        finally
        {
            refCounted.decref();
        }

        return map;
    }

    /**
     * Get indexed fields in Solr.
     * The list of fields might be not complete in a sharded environment.
     * @return
     * @throws RuntimeException
     */
    private Map<String, String> getIndexedFieldsInfo() throws RuntimeException
    {

        RefCounted<SolrIndexSearcher> refCounted = core.getSearcher();
        SolrIndexSearcher searcher;
        try {
            searcher = refCounted.get();
            LeafReader reader = searcher.getSlowAtomicReader();
            IndexSchema schema = searcher.getSchema();

            Set<String> fieldNames = new TreeSet<>();
            for (FieldInfo fieldInfo : reader.getFieldInfos()) {
                fieldNames.add(fieldInfo.name);
            }
            Map<String, String> fieldMap = new HashMap<>();
            for (String fieldName : fieldNames) {
                SchemaField sfield = schema.getFieldOrNull(fieldName);
                FieldType ftype = (sfield == null) ? null : sfield.getType();

                String alfrescoPropertyFromSchemaField = null;
                try
                {
                    alfrescoPropertyFromSchemaField = AlfrescoSolrDataModel.getInstance().getAlfrescoPropertyFromSchemaField(fieldName);
                }
                catch (NamespaceException ne)
                {
                    //Field name may have been created but now deactivated, e.g custom model.
                    LOGGER.warn("Unable to resolve field: " + fieldName);
                }

                if (isNotBlank(alfrescoPropertyFromSchemaField) && ftype != null)
                {
                    String className = ftype.getClassArg();
                    if (isNotBlank(className))
                    {
                        // Add the field
                        fieldMap.put(alfrescoPropertyFromSchemaField, className);
                    }
                }
            }

            return fieldMap;
        } finally {
            refCounted.decref();
        }
    }

}
