/*
 * Copyright (C) 2005-2018 Alfresco Software Limited.
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
package org.alfresco.solr.sql;


import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Collections;
import java.util.HashMap;

import org.alfresco.solr.AlfrescoSolrDataModel;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class for all Solr schema related functions.
 * @author Michael Suzuki
 *
 */
public class SolrSchemaUtil 
{

    private static final Logger LOGGER = LoggerFactory.getLogger(SolrSchema.class);

    /**
     * Regex for retrieving custom fields definitions (i.e. name and type) from shared.properties
     */
    private static final String SOLR_SQL_ALFRESCO_FIELDNAME_REGEXP = "solr\\.sql\\.alfresco\\.fieldname\\..*";

    /**
     * Extract predicates of dynamic properties, such as custom model, to help
     *  build a complete table when select start is used.
     * 
     * @param sql String query
     * @return {@link Set} of predicates
     */
    public static Set<String> extractPredicates(String sql)
    {
        if(!StringUtils.isEmpty(sql))
        {
            if(sql.toLowerCase().contains("where"))
            {
                Set<String> predicates = new HashSet<String>();
                //Strip NOT,not and Not and split on WHERE,where and Where.
                String[] sqlpred = sql.replaceAll(" (?i)not ", " ").split(" (?i)where ");
                String[] conjunctionAndDisjunction = sqlpred[1].split("(?i)and | or");
                for(int i = 0; i < conjunctionAndDisjunction.length; i++)
                {
                    String predic = conjunctionAndDisjunction[i].split("[><!~]=?|<>|=| (?i)in | (?i)between ")[0].trim();
                    if(!predic.startsWith("'"))
                    {
                        predicates.add(predic.replaceAll("`", ""));
                    }
                }
                return predicates;
            }
        }
        return Collections.emptySet();
    }

    /**
     * This methods extracts a set of custom fields (including type) from the shared properties.
     */
    public static Map<String, String> fetchCustomFieldsFromSharedProperties()
    {
        Map<String, String> collection = new HashMap<>();
        Properties properties = AlfrescoSolrDataModel.getCommonConfig();
        properties.forEach((key, value) -> {
            String label = (String) key;
            String fieldValue = (String) value;
            //Match on solr.sql.tablename.field.name=nameValue
            if (label.matches(SOLR_SQL_ALFRESCO_FIELDNAME_REGEXP))
            {
                String val = label.replace("fieldname", "fieldtype");
                String type = (String) properties.get(val);
                if (type == null)
                {
                    LOGGER.error("Type definition: " + val + " not found in the shared.properties");
                }
                else
                {
                    collection.put(fieldValue, type);
                }
            }
        });
        return collection;
    }
}
