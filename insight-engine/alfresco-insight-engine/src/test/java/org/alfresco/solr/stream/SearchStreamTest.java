package org.alfresco.solr.stream;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

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
    public void rewriteFlWithNumberPrefixes() throws IOException {
        cut = new SearchStream("","", new ModifiableSolrParams());

        String fl = "aField,1_genre,b123,2_somethingelse,*_aGlob,c,alias:(sum(1,prod(popularity,1)))[explain]";
        String expected = "aField,*_genre,b123,*_somethingelse,*_aGlob,c,alias:(sum(1,prod(popularity,1)))[explain],[cached]";

        assertEquals(expected, cut.withRewrite(fl));
    }
}
