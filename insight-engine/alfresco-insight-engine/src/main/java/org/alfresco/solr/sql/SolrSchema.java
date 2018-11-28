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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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


public class SolrSchema extends AbstractSchema {

    private static final String SOLR_SQL_ALFRESCO_FIELDNAME_REGEXP = "solr.sql.alfresco.*.fieldname.*=*";
    private static Logger logger = LoggerFactory.getLogger(SolrSchema.class);
    final public static Set<String> selectStarFieldsDefault = new HashSet<String>(Arrays
        .asList("cm_name", "cm_created", "cm_creator", "cm_modified", "cm_modifier", "cm_owner", "OWNER", "TYPE", "LID",
            "DBID", "cm_title", "cm_description", "cm_content.size", "cm_content.mimetype", "cm_content.encoding",
            "cm_content.locale", "cm_lockOwner", "SITE", "PRIMARYPARENT", "PARENT", "PATH", "ASPECT", "QNAME"));
    final Properties properties;
    final SolrCore core;
    final String[] postfixes = { "_day", "_month", "_year" };
    final boolean isSelectStar;
    final Set<String> selectStarFields = new HashSet<String>();
    final Set<Map.Entry<String, String>> customFieldsFromSharedProperties = new HashSet<Map.Entry<String, String>>();

    SolrSchema(SolrCore core, Properties properties) {
    super();
    this.core = core;
    this.properties = properties;
    this.isSelectStar = Boolean.parseBoolean(properties.getProperty(AlfrescoSQLHandler.IS_SELECT_STAR));
        fetchCustomFieldsFromSharedProperties();
    if(isSelectStar) {
        selectStarFields.addAll(selectStarFieldsDefault);
        String sql = properties.getProperty("stmt", "");
        //Add dynamic fields not part of the schema such as custom models and aspects.
        if(predicateExists(sql))
        {
            SolrSchemaUtil.extractPredicates(sql).forEach(action -> selectStarFields.add(action));
        }
    }
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

    RelProtoDataType getRelDataType(String collection)
    {

        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
        //Add fields from schema.
        Map<String, String> fieldsAndTypeFromSolrIndex = getIndexedFieldsInfo();
        boolean isDate = false;

        Set<Map.Entry<String, String>> fieldsAndTypeEntriesFromSolrIndex = fieldsAndTypeFromSolrIndex.entrySet();
        boolean hasLockOwner = false;

        for (Map.Entry<String, String> fieldAndTypeFromSolrIndex : fieldsAndTypeEntriesFromSolrIndex)
        {
            String fieldTypeFromSolrIndex = fieldAndTypeFromSolrIndex.getValue();
            RelDataType type;
            if (!hasLockOwner)
            {
                //Check to see if the field exists to avoid duplicating the field.
                hasLockOwner = lockOwnerFieldExists(fieldAndTypeFromSolrIndex.getKey());
            }
            type = resolveType(fieldTypeFromSolrIndex, typeFactory);
            addFieldInfo(fieldInfo, fieldAndTypeFromSolrIndex, type, null);
            if (isDate)
            {
                isDate = false;
                addTimeFields(fieldInfo, fieldAndTypeFromSolrIndex, typeFactory.createJavaType(String.class));
            }
        }

        /**
         * Load mandatory fields that have not already been loaded
         */
        for (Map.Entry<String, String> field : customFieldsFromSharedProperties)
        {
            fieldInfo.add(field.getKey(), resolveType(field.getValue(), typeFactory));
        }
        fieldInfo.add("_query_", typeFactory.createJavaType(String.class));
        fieldInfo.add("score", typeFactory.createJavaType(Double.class));

        if (!hasLockOwner)
        {
            //Add fields that might be queried on that does not exist yet.
            fieldInfo.add("cm:lockOwner", typeFactory.createJavaType(String.class));
            fieldInfo.add("cm_lockOwner", typeFactory.createJavaType(String.class));
        }
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
    for(String postfix : postfixes) {
      addFieldInfo(fieldInfo, entry, type, postfix);
    }
  }

  private void addFieldInfo(RelDataTypeFactory.FieldInfoBuilder fieldInfo, Map.Entry<String, String> entry, RelDataType type, String postfix) {

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
      
      if(isSelectStar) 
      {
          if ((!selectStarFields.contains(entry.getKey()) && !selectStarFields.contains(formatted)) && (
              !customFieldsFromSharedProperties.contains(entry.getKey()) && !customFieldsFromSharedProperties
                  .contains(formatted)))
          {
              return;
          }
      }
      fieldInfo.add(entry.getKey()+getPostfix(postfix), type).nullable(true);
      if(!formatted.contentEquals(entry.getKey() + getPostfix(postfix)))
      {
          fieldInfo.add(formatted, type).nullable(true);
      }
  }

  private String getPostfix(String postfix) {
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
              logger.warn("Unable to resolve field: " + fieldName);
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

    /**
     * This methods extracts a set of custom fields (including type) from the shared properties.
     */
    private void fetchCustomFieldsFromSharedProperties()
    {
        Properties properties = AlfrescoSolrDataModel.getCommonConfig();
        properties.forEach((key, value) -> {
            String label = (String) key;
            String fieldValue = (String) value;
            //Match on solr.sql.tablename.field.name=nameValue
            if (label.matches(SOLR_SQL_ALFRESCO_FIELDNAME_REGEXP))
            {
                String val = label.replace("fieldname", "fieldtype");
                String type = (String) properties.get(val);
                customFieldsFromSharedProperties.add(new AbstractMap.SimpleEntry(fieldValue, type));
                if (fieldValue.contains(":"))
                {
                    customFieldsFromSharedProperties
                        .add(new AbstractMap.SimpleEntry(fieldValue.replaceAll(":", "_"), type));
                }
            }
        });
    }

    public boolean predicateExists(String sql)
  {
      
      if(sql != null && sql.toLowerCase().contains("where"))
      {
          return true;
      }
      return false;
  }
}
