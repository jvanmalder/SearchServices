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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

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
* The SolrSchema class creates the "alfresco" table and populates the fields from the index.
*/
public class SolrSchema extends AbstractSchema
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrSchema.class);



    /**
     * The default type we assign to fields not explicitly declared (i.e. defined in shared.properties or hard coded in select star fields).
     * Using the StrField as default type allows the SQL processor to manage them as opaque literals, without any further parsing.
     * In this way the (String) literal processing is moved completely on Solr side.
     */
    public static final String UNKNOWN_FIELD_DEFAULT_TYPE = "solr.StrField";

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
    final Map<String, String> additionalFieldsFromConfiguration = new HashMap<>();

    SolrSchema(SolrCore core, Properties properties) {
        super();
        this.core = core;
        this.properties = properties;
        this.isSelectStarQuery = Boolean.parseBoolean(properties.getProperty(AlfrescoSQLHandler.IS_SELECT_STAR));

        initFieldsFromConfiguration(properties);
    }

    /**
     * Init a map of field-> type from :
     * - configurations defined in the shared properties
     * - hard coded list of select star fields
     * - fields from the sql predicates
     *
     * @param properties
     */
    private void initFieldsFromConfiguration(Properties properties)
    {
        additionalFieldsFromConfiguration.putAll(SolrSchemaUtil.fetchCustomFieldsFromSharedProperties());

        if (isSelectStarQuery)
        {
            SelectStarDefaultField[] defaultSelectStarFields = SelectStarDefaultField.values();
            for (SelectStarDefaultField fieldAndType : defaultSelectStarFields)
            {
                additionalFieldsFromConfiguration.putIfAbsent(fieldAndType.getFieldName(), fieldAndType.getFieldType());
            }
        }
        String sql = properties.getProperty("stmt", "");
        //Add dynamic fields not part of the schema such as custom models and aspects.
        if (predicateExists(sql))
        {
            SolrSchemaUtil.extractPredicates(sql).forEach(
                fieldName -> additionalFieldsFromConfiguration.putIfAbsent(fieldName, UNKNOWN_FIELD_DEFAULT_TYPE));
        }
    }

    public boolean predicateExists(String sql)
    {
        return (sql != null && sql.toLowerCase().contains(" where "));
    }
    
    @Override
  protected Map<String, Table> getTableMap() {
    Map<String, Table> map = new HashMap<String, Table>();
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
        //Add fields from local index
        Map<String, String> fieldsAndTypeFromSolrIndex = getIndexedFieldsInfo();
        Set<Map.Entry<String, String>> fieldsAndTypeEntriesFromSolrIndex = fieldsAndTypeFromSolrIndex.entrySet();

        for (Map.Entry<String, String> fieldAndTypeFromSolrIndex : fieldsAndTypeEntriesFromSolrIndex)
        {
            String fieldTypeFromSolrIndex = fieldAndTypeFromSolrIndex.getValue();
            RelDataType type;
            type = resolveType(fieldTypeFromSolrIndex, typeFactory);
            addFieldInfoOriginalNameAndFormatted(fieldInfo, fieldAndTypeFromSolrIndex, type, null, null);
            if (fieldTypeFromSolrIndex.equals("java.util.Date")||fieldTypeFromSolrIndex.equals("solr.TrieDateField"))
            {
                addTimeFields(fieldInfo, fieldAndTypeFromSolrIndex, typeFactory.createJavaType(String.class));
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

private void addTimeFields(RelDataTypeFactory.FieldInfoBuilder fieldInfo, Map.Entry<String, String> entry, RelDataType type) {
    for(String postfix : postfixes)
    {
        addFieldInfoOriginalNameAndFormatted(fieldInfo, entry, type, postfix, null);
    }
}

    private void addFieldInfoOriginalNameAndFormatted(RelDataTypeFactory.FieldInfoBuilder fieldInfo, Entry<String, String> entry, RelDataType type, String postfix, String formattedFieldName)
    {
        if(formattedFieldName==null)
        {
            formattedFieldName = getFormattedFieldName(entry, postfix);
        }

        if (isSelectStarQuery){
            if(!additionalFieldsFromConfiguration.keySet().contains(entry.getKey())
                    && !additionalFieldsFromConfiguration.keySet().contains(formattedFieldName)) {
                return;
            }
        }

        fieldInfo.add(entry.getKey() + getPostfix(postfix), type).nullable(true);
        if (!formattedFieldName.contentEquals(entry.getKey() + getPostfix(postfix)))
        {
            fieldInfo.add(formattedFieldName, type).nullable(true);
        }
    }

    private String getFormattedFieldName(Entry<String, String> entry, String postfix)
    {
        String formatted = entry.getKey();
        try
        {
            String[] withPrefix = QName.splitPrefixedQName(entry.getKey());
            String prefix = withPrefix[0];
            if (prefix != null && !prefix.isEmpty())
            {
                formatted = withPrefix[0]+"_"+withPrefix[1]+getPostfix(postfix);
            }
            
            //Potentially remove prefix, just shortname if unique
            //QueryParserUtils.matchPropertyDefinition will throw an error if duplicate
            
        } catch (NamespaceException e) {
            //ignore invalid qnames
        }
        return formatted;
    }

    private String getPostfix(String postfix)
    {
    if(postfix != null) {
      return postfix;
    } else {
      return "";
    }
  }

  private Map<String, String> getIndexedFieldsInfo() throws RuntimeException {

    RefCounted<SolrIndexSearcher> refCounted = core.getSearcher();
    SolrIndexSearcher searcher = null;
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
