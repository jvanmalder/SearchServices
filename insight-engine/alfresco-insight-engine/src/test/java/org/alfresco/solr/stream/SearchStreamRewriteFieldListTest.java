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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test case for {@link SearchStream#withRewrite(String)} method, used for rewriting the fl parameter in case fields
 * start with a number.
 *
 * The tests methods tries to check the rewrite behaviour using all the capabilities supported by the fl (parser) parameter:
 *
 * <ul>
 *     <li>field names (aField, name, genre)</li>
 *     <li>glob expressions (n?me,*nre). See org.apache.commons.io.FilenameUtils#wildcardMatch(String, String) for the matching algorithm.</li>
 *     <li>functions (sum(1,1))</li>
 *     <li>namespaces (namespace:fieldname,namespace:sum(1,popularity)</li>
 * </ul>
 *
 * Note: the current {@link SearchStream} implementation doesn't make use of functions, aliases and glob expressions.
 * Specifically for aliases:since the code is using a "namespaced" field syntax (e.g. cm:name) the Solr built-in alias capability
 * cannot be used (colons are replaced with underscores before sending the final request to Solr).
 *
 * Known limitations in case of fields starting with numbers and / or containing hyphen(-) or plus(+) chars:
 *
 * <ul>
 *     <li>They cannot be used within a function, e.g. sum(1_genre, 1)</li>
 *     <li>Aliases cannot have a numeric prefix and cannot contain hyphen / plus chars, e.g 1a-l-i-a-s:aFieldName</li>
 * </ul>
 *
 *
 * @see org.apache.commons.io.FilenameUtils#wildcardMatch(String, String)
 */
public class SearchStreamRewriteFieldListTest
{

    private SearchStream searchStream;
    private Map<String, String> inputAndExpectedPairs;

    @Before
    public void setUp() throws IOException
    {
        searchStream = new SearchStream("","", new ModifiableSolrParams());
        inputAndExpectedPairs = new HashMap<>();
    }

    /**
     * If a fieldname doesn't start with a number and doesn't contain any hyphen then no rewrite should happen.
     */
    @Test
    public void dontRewrite()
    {
        inputAndExpectedPairs.put(
                "this_is_a_field123,this_is_another321_field",
                "this_is_a_field123,this_is_another321_field");

        assertRewriteCorrectness();
    }

    /**
     * Each plus or minus (hyphen) character in the fieldname must be replaced with a question mark.
     * This must happen regardless the fieldname prefix (numeric or not).
     */
    @Test
    public void fieldNameContainsPlusOrMinusChar()
    {
        inputAndExpectedPairs.put(
                "this-is+a+field-name,and+this+is+another",
                "this?is?a?field?name,and?this?is?another");

        assertRewriteCorrectness();
    }

    /**
     * If the first char of a field is a digit, then it must be replaced with a question mark.
     */
    @Test
    public void fieldNameStartingWithNumber()
    {
        inputAndExpectedPairs.put(
                "1this_is_a_field123,12this_is_another321_field,1_another_one",
                "?this_is_a_field123,?2this_is_another321_field,?_another_one");

                assertRewriteCorrectness();
    }

    /**
     * If the first char is a digit it must be replaced with a question mark.
     * In addition, any plus or minus character must be replaced as well.
     */
    @Test
    public void fieldNameStartingWithNumberAndPlusOrMinusCharsInTheMiddle()
    {
        inputAndExpectedPairs.put(
                "1this+is-a+field123,12this+is_another321-field,1--another_one",
                "?this?is?a?field123,?2this?is_another321?field,???another_one");

        assertRewriteCorrectness();
    }

    /**
     * Mixed cases of fields that don't start with a number with:
     *
     * <ul>
     *     <li>functions</li>
     *     <li>aliases</li>
     *     <li>transformers</li>
     * </ul>
     */
    @Test
    public void rewriteWithoutNumericPrefixedFields()
    {
        // 1. one field ok, one function, one field to rewrite
        inputAndExpectedPairs.put(
                "aField,sum(field1,1),a-Field",
                "aField,sum(field1,1),a?Field");

        // 2. one field to rewrite, two aliased functions, one field ok, another function (not aliased)
        inputAndExpectedPairs.put(
                "a-F-i-e-l-d,ns1:(sum(1,1)),ns2:sum(rating,1),aField,sum(popularity,1)",
                "a?F?i?e?l?d,ns1_(sum(1,1)),ns2_sum(rating,1),aField,sum(popularity,1)");

        // 3. nothing to rewrite
        inputAndExpectedPairs.put(
                "*_globExpression1,?_globExpression2,*glob?expression3,[aTransformer]",
                "*_globExpression1,?_globExpression2,*glob?expression3,[aTransformer]");

        // 4. a constant score function, a glob expression and a field to rewrite
        inputAndExpectedPairs.put(
                "-18.2,*globExpression?,aFiel-d",
                "-18.2,*globExpression?,aFiel?d");

        assertRewriteCorrectness();
    }

    /**
     * Mixed cases of fields that start with a number, plus:
     *
     * <ul>
     *     <li>functions</li>
     *     <li>aliases</li>
     *     <li>transformers</li>
     * </ul>
     */
    @Test
    public void rewriteWithNumericPrefixedFields()
    {
        // 1. fields mixed with aliased and non aliased functions
        inputAndExpectedPairs.put(
                "1aField,ns1:(sum(1,1)),ns2:sum(popularity,1),23_aField,sum(rating,1)",
                "?aField,ns1_(sum(1,1)),ns2_sum(popularity,1),?3_aField,sum(rating,1)");

        // 2. fields mixed with transformers and glob expressions
        inputAndExpectedPairs.put(
                "*_globExpression1,1Field,?_globExpression2,2Field8,Field9,*glob?expression3,[explain],[docid]",
                "*_globExpression1,?Field,?_globExpression2,?Field8,Field9,*glob?expression3,[explain],[docid]");

        // 3. constants score function and glob expression
        inputAndExpectedPairs.put(
                "-18.2,*globExpression?,aField8,92_Field3",
                "-18.2,*globExpression?,aField8,?2_Field3");

        assertRewriteCorrectness();
    }

    /**
     * Asserts the correctness of the fl rewrite parameter using an expectation map whose entries are composed by
     * an input values and the corresponding expectation in terms of rewrite.
     */
    private void assertRewriteCorrectness()
    {
        // 1. Cached transformer not in fields list
        inputAndExpectedPairs.forEach((input, expected) -> assertEquals(expected + ",[cached]", searchStream.withRewrite(input)));

        // 2. Cached transformer at the end of the fields list
        inputAndExpectedPairs.values().stream()
                .map(value -> value.concat(",[cached]"))
                .forEach(fl -> assertEquals(fl, searchStream.withRewrite(fl)));

        // 3. Cached transformer at the beginning of the fields list
        inputAndExpectedPairs.values().stream()
                .map(value -> "[cached]," + value)
                .forEach(fl -> assertEquals(fl, searchStream.withRewrite(fl)));

        // 4. Cached transformer in the middle of the fields list
        inputAndExpectedPairs.values().stream()
                .map(value -> value.replaceFirst(",", ",[cached]"))
                .forEach(fl -> assertEquals(fl, searchStream.withRewrite(fl)));
    }
}