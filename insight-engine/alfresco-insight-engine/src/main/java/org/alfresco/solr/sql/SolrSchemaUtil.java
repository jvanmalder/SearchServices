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

import org.alfresco.solr.AlfrescoSolrDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Util class for all Solr schema related functions.
 *
 * @author Michael Suzuki
 */
public class SolrSchemaUtil 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrSchema.class);

    /**
     * Custom fields definition from shared.properties
     */
    private static final String SOLR_SQL_ALFRESCO_FIELDNAME = "solr.sql.alfresco.fieldnames";

    /**
     * This methods extracts a set of custom fields (including type) from the shared properties.
     */
    public static Set<String> fetchCustomFieldsFromSharedProperties()
    {
        Set<String> collection = new HashSet<>();
        Properties properties = AlfrescoSolrDataModel.getCommonConfig();
        properties.forEach((key, value) -> {
            String label = (String) key;
            String fieldValue = (String) value;
            if (label.equals(SOLR_SQL_ALFRESCO_FIELDNAME))
            {
                for (String fieldName : fieldValue.replaceAll("\\s+","").split(","))
                {
                    collection.add(fieldName);
                }
            }
        });
        return collection;
    }
}