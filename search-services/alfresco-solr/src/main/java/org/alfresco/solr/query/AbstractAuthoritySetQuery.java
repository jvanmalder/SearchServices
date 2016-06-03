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
package org.alfresco.solr.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.alfresco.repo.search.adaptor.lucene.QueryConstants;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Base class for queries relating to a set of authorities, e.g. reader set query.
 */
public abstract class AbstractAuthoritySetQuery extends Query
{
    protected String authorities;

    /**
     * Construct with authorities.
     * 
     * @param authorities
     */
    public AbstractAuthoritySetQuery(String authorities)
    {
        super();
        this.authorities = authorities;
    }

    @Override
    public abstract Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException;
    
    @Override
    public String toString(String field)
    {
        return toString();
    }
    
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((authorities == null) ? 0 : authorities.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractAuthoritySetQuery other = (AbstractAuthoritySetQuery) obj;
        if (authorities == null)
        {
            if (other.authorities != null)
                return false;
        }
        else if (!authorities.equals(other.authorities))
            return false;
        return true;
    }

    protected HybridBitSet getACLSet(String[] auths, String field, SolrIndexSearcher searcher) throws IOException
    {
    	BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for(String current : auths)
        {
        	queryBuilder.add(new TermQuery(new Term(field, current)), BooleanClause.Occur.SHOULD);
        }

        //NOTE: this query will be in the filter cache. Ideally it would remain cached throughout the users session.
        DocSet docSet = searcher.getDocSet(queryBuilder.build());

        DocIterator iterator = docSet.iterator();
        if(!iterator.hasNext())
        {
            return new EmptyHybridBitSet();
        }

        //TODO : makes this configurable. For some systems this is huge and for others not big enough.
        HybridBitSet hybridBitSet = new HybridBitSet(60000000);

        List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
        LeafReaderContext context = leaves.get(0);
        NumericDocValues aclValues = DocValuesCache.getNumericDocValues(QueryConstants.FIELD_ACLID, context.reader());
        LeafReader reader = context.reader();
        int ceil = reader.maxDoc();
        int base = 0;
        int ord = 0;
        while (iterator.hasNext()) {
            int doc = iterator.nextDoc();
            if(doc >= ceil)
            {
                do
                {
                    ++ord;
                    context = leaves.get(ord);
                    reader = context.reader();
                    base = context.docBase;
                    ceil = base+reader.maxDoc();
                    aclValues = DocValuesCache.getNumericDocValues(QueryConstants.FIELD_ACLID, reader);
                }
                while(doc >= ceil);
            }

            if(aclValues != null) {
                long aclId = aclValues.get(doc - base);
                hybridBitSet.set(aclId);
            }
        }

        return hybridBitSet;
    }

    protected BitsFilter getACLFilter(String[] auths, String field, SolrIndexSearcher searcher) throws IOException
    {
        HybridBitSet aclBits = getACLSet(auths, field, searcher);
        List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
        List<FixedBitSet> bitSets = new ArrayList<FixedBitSet>(leaves.size());

        for(LeafReaderContext readerContext :  leaves)
        {
        	LeafReader reader = readerContext.reader();
            int maxDoc = reader.maxDoc();
            FixedBitSet bits = new FixedBitSet(maxDoc);
            bitSets.add(bits);

            NumericDocValues fieldValues = DocValuesCache.getNumericDocValues(QueryConstants.FIELD_ACLID, reader);
            if (fieldValues != null) {
                for (int i = 0; i < maxDoc; i++) {
                    long aclID = fieldValues.get(i);
                    if (aclBits.get(aclID)) {
                        bits.set(i);
                    }
                }
            }
        }

        return new BitsFilter(bitSets);
    }

}