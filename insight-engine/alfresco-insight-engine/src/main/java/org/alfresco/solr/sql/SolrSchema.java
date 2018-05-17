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

import java.util.*;

import org.alfresco.opencmis.dictionary.CMISStrictDictionaryService;
import org.alfresco.service.cmr.dictionary.PropertyDefinition;
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
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.handler.AlfrescoSQLHandler;

class SolrSchema extends AbstractSchema {
  final Properties properties;
  final SolrCore core;
  final String[] postfixes = {"_day", "_month", "_year"};
  final Set selectStarFields = new HashSet();
  final boolean isSelectStar;

  SolrSchema(SolrCore core, Properties properties) {
    super();
    this.core = core;
    this.properties = properties;
    this.isSelectStar = Boolean.parseBoolean(properties.getProperty(AlfrescoSQLHandler.IS_SELECT_STAR));
    if(isSelectStar) {
        addSelectStarFields();
    }
  }

  @Override
  protected Map<String, Table> getTableMap() {
    Map<String, Table> map = new HashMap();
    map.put("alfresco", new SolrTable(this, "alfresco"));
    return map;
  }


  RelProtoDataType getRelDataType(String collection) {

    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
    //Add fields from schema.
    Map<String, String> fields = getIndexedFieldsInfo();
    boolean isDate = false;


    Set<Map.Entry<String, String>> set = fields.entrySet();

    for(Map.Entry<String, String> entry : set) {
        String ltype = entry.getValue();
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
                isDate = true;
                type = typeFactory.createJavaType(String.class);
                break;
            case "java.util.Date":
                isDate = true;
                type = typeFactory.createJavaType(String.class);
                break;
            default:
              type = typeFactory.createJavaType(String.class);
        }
        addFieldInfo(fieldInfo, entry, type, null);
        if(isDate)
        {
            isDate = false;
            addTimeFields(fieldInfo, entry, typeFactory.createJavaType(String.class));
        }
    }

    fieldInfo.add("_query_",typeFactory.createJavaType(String.class));
    fieldInfo.add("score", typeFactory.createJavaType(Double.class));

    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  private void addTimeFields(RelDataTypeFactory.FieldInfoBuilder fieldInfo, Map.Entry<String, String> entry, RelDataType type) {
    for(String postfix : postfixes) {
      addFieldInfo(fieldInfo, entry, type, postfix);
    }
  }

  private void addFieldInfo(RelDataTypeFactory.FieldInfoBuilder fieldInfo, Map.Entry<String, String> entry, RelDataType type, String postfix) {

      if(isSelectStar) {
        if(!selectStarFields.contains(entry.getKey())) {
            return;
        }
      }

    fieldInfo.add(entry.getKey()+getPostfix(postfix), type).nullable(true);

    try
    {
      String[] withPrefix = QName.splitPrefixedQName(entry.getKey());
      String prefix = withPrefix[0];
      if (prefix != null && !prefix.isEmpty())
      {
        fieldInfo.add(withPrefix[0]+"_"+withPrefix[1]+getPostfix(postfix), type).nullable(true);
      }

      //Potentially remove prefix, just shortname if unique
      //QueryParserUtils.matchPropertyDefinition will throw an error if duplicate

    } catch (NamespaceException e) {
      //ignore invalid qnames
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
      Map fieldMap = new HashMap();
      for (String fieldName : fieldNames) {
        SchemaField sfield = schema.getFieldOrNull(fieldName);
        FieldType ftype = (sfield == null) ? null : sfield.getType();

        // Add the field
        fieldMap.put(AlfrescoSolrDataModel.getInstance().getAlfrescoPropertyFromSchemaField(fieldName), ftype.getClassArg());
      }

      return fieldMap;
    } finally {
      refCounted.decref();
    }
  }

  private void addSelectStarFields() {
      selectStarFields.add("DBID");
      selectStarFields.add("OWNER");
      selectStarFields.add("cm:title");
      selectStarFields.add("cm:created");
      selectStarFields.add("cm:owner");
  }

}
