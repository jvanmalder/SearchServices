/*-
 * #%L
 * Alfresco Remote API
 * %%
 * Copyright (C) 2005 - 2016 Alfresco Software Limited
 * %%
 * This file is part of the Alfresco software.
 * If the software was purchased under a paid Alfresco license, the terms of
 * the paid license agreement will prevail.  Otherwise, the software is
 * provided under the following open source license terms:
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
 * #L%
 */
package org.alfresco.solr.stream;

import static org.junit.Assert.assertEquals;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

import java.io.IOException;

/**
 * Test case for {@link SearchStream}.
 */
public class SearchStreamTest {
    private SearchStream cut;

    /**
     * If the fl parameter doesn't contain fields starting with numbers then it has to be left untouched.
     */
    @Test
    public void rewriteFlWithNoNumericPrefixedFields() throws IOException {
        cut = new SearchStream("","", new ModifiableSolrParams());

        final String fl = "aField,b123,*_aGlob,c,[explain],alias:(sum(1,1))";

        assertEquals(fl + ",[cached]", cut.withRewrite(fl));
    }

    @Test
    public void rewriteFlWithNumericPrefixedFields() throws IOException {
        cut = new SearchStream("","", new ModifiableSolrParams());

        String fl = "aField,1_genre,b123,2_somethingelse,?_aGlob,c,alias:(sum(1,prod(popularity,1)))[explain]";
        String expected = "aField,?_genre,b123,?_somethingelse,?_aGlob,c,alias:(sum(1,prod(popularity,1)))[explain],[cached]";

        assertEquals(expected, cut.withRewrite(fl));
    }

    @Test
    public void rewriteWithCachedTranformerInFl() throws IOException {
        cut = new SearchStream("","", new ModifiableSolrParams());

        String fl1 = "aField,1_genre,b123,2_somethingelse,*_aGlob,c,alias:(sum(1,prod(popularity,1)))[explain],[cached]";
        String expected1 = "aField,?_genre,b123,?_somethingelse,*_aGlob,c,alias:(sum(1,prod(popularity,1)))[explain],[cached]";

        assertEquals(expected1, cut.withRewrite(fl1));

        String fl2 = "-1,aField,1_genre,b123,[cached],2_somethingelse,*_aGlob,c,alias:(sum(1,prod(popularity,1)))[explain]";
        String expected2 = "-1,aField,?_genre,b123,[cached],?_somethingelse,*_aGlob,c,alias:(sum(1,prod(popularity,1)))[explain]";

        assertEquals(expected2, cut.withRewrite(fl2));
    }
}
