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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
/**
 * Util class for all Solr schema related functions.
 * @author Michael Suzuki
 *
 */
public class SolrSchemaUtil 
{
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
                String[] sqlpred = sql.replaceAll("(?i)not", "").split("(?i)where");
                String[] conjunctionAndDisjunction = sqlpred[1].split("(?i)and | or");
                for(int i = 0; i < conjunctionAndDisjunction.length; i++)
                {
                    String predic = conjunctionAndDisjunction[i].split("[><!~]=? |<>|=")[0].trim();
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
}
