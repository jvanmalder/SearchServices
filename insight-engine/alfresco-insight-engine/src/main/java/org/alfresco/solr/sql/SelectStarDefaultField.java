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

public enum SelectStarDefaultField
{
    CM_NAME("cm:name", "solr.StrField"),
    CM_CREATED("cm:created", "solr.TrieDateField"),
    CM_CREATOR("cm:creator", "solr.StrField"),
    CM_MODIFIED("cm:modified", "solr.TrieDateField"),
    CM_MODIFIER("cm:modifier", "solr.StrField"),
    CM_OWNER("cm:owner", "solr.StrField"),
    OWNER("OWNER", "solr.StrField"),
    TYPE("TYPE", "solr.StrField"),
    LID("LID", "solr.StrField"),
    DBID("DBID", "solr.TrieLongField"),
    CM_TITLE("cm:title", "solr.StrField"),
    CM_DESCRIPTION("cm:description", "solr.StrField"),
    CM_CONTENT_SIZE("cm:content.size", "solr.TrieLongField"),
    CM_CONTENT_MIMETYPE("cm:content.mimetype", "solr.StrField"),
    CM_CONTENT_ENCODING("cm:content.encoding", "solr.StrField"),
    CM_CONTENT_LOCALE("cm:content.locale", "solr.StrField"), 
    CM_LOCKOWNER("cm:lockOwner", "solr.StrField"),
    SITE("SITE", "solr.StrField"),
    PRIMARYPARENT("PRIMARYPARENT", "solr.StrField"),
    PARENT("PARENT", "solr.StrField"),
    PATH("PATH", "solr.StrField"),
    ASPECT("ASPECT", "solr.StrField"),
    QNAME("QNAME", "solr.StrField");
    
    private final String fieldName;
    private final String fieldType;

    SelectStarDefaultField(String fieldName, String fieldType) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
        }

    public String getFieldName()
    {
        return fieldName;
    }

    public String getFieldType()
    {
        return fieldType;
    }
}
