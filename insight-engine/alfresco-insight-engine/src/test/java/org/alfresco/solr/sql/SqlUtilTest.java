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

import static org.alfresco.util.collections.CollectionUtils.asSet;
import static org.junit.Assert.assertFalse;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Test case for {@link SqlUtil}.
 * The "select star detection" tests requires two separate categories of "select" queries (select with and without order by clause) because the
 * Calcite query parser produces a composite tree node where (sometimes) the top level node is an OrderBy node (that includes a Select node).
 *
 * @author agazzarini
 * @since 1.1
 */
public class SqlUtilTest
{
    private List<String> selectStarQueries =
            asList(
                    "SELECT * FROM table_name WHERE ID=1",
                    "SELECT *,City FROM Supplier",
                    "SELECT *,City as CityAlias FROM Supplier WHERE Country IN ('USA', 'Japan', 'Germany')",
                    // Same queries with some whitespace chars
                    "SELECT    *     FROM table_name WHERE ID=1",
                    "SELECT \n\n\n\r*\n\n\f,City FROM Supplier",
                    "SELECT \n\n\n\f*  ,     City     as CityAlias FROM Supplier WHERE Country IN ('USA', 'Japan', 'Germany')",
                    "select\n" +
                            "*\n" +
                            "from\n" +
                            "alfresco\n" +
                            "where\n" +
                            "`finance:Emp`\n" +
                            "in\n" +
                            "(10, 30, 0)",
                    "select\r" +
                            "*\r" +
                            "from\r" +
                            "alfresco\r" +
                            "where\r" +
                            "`finance:Emp`\r" +
                            "in\r" +
                            "(10, 30, 0)");

    private List<String> selectStarQueriesWithOrderBy =
            asList(
                    "SELECT * FROM table_name WHERE ID=1 ORDER BY column_names",
                    "SELECT *,City FROM Supplier ORDER BY CompanyName asc",
                    "SELECT *,City as CityAlias FROM Supplier WHERE Country IN ('USA', 'Japan', 'Germany') ORDER BY CompanyName asc, Country desc, ContactName asc",
                    // Same queries with some whitespace chars
                    "SELECT \n\n\t*    FROM table_name WHERE ID=1 ORDER BY column_names",
                    "SELECT \t\t\t*,   \n\rCity FROM Supplier ORDER BY CompanyName asc",
                    "SELECT \n*,   \tCity as CityAlias FROM Supplier WHERE Country IN ('USA', 'Japan', 'Germany') ORDER BY CompanyName asc, Country desc, ContactName asc");

    private List<String> nonSelectStarQueries =
            asList(
                    "SELECT column_names FROM table_name WHERE ID=1",
                    "SELECT CompanyName, ContactName, City, Country FROM Supplier",
                    "SELECT Id, CompanyName, City, Country FROM Supplier WHERE Country IN ('USA', 'Japan', 'Germany')",
                    "SELECT COUNT(Id), Country  FROM Customer GROUP BY Country",
                    // Same queries with some whitespace chars
                    "SELECT\n\n\rcolumn_names\tFROM table_name WHERE ID=1",
                    "SELECT CompanyName,\r\fContactName, City, Country FROM Supplier",
                    "SELECT Id, CompanyName,\n\n City, Country FROM Supplier WHERE Country IN ('USA', 'Japan', 'Germany')",
                    "SELECT COUNT(Id),\r\r\n Country  FROM Customer GROUP BY Country");

    private List<String> nonSelectStarQueriesWithOrderBy =
            asList(
                    "SELECT column_names FROM table_name WHERE ID=1 ORDER BY column_names",
                    "SELECT CompanyName, ContactName, City, Country FROM Supplier ORDER BY CompanyName asc",
                    "SELECT CompanyName, ContactName, City, Country FROM Supplier ORDER BY CompanyName asc, Country desc, ContactName asc",
                    "SELECT COUNT(Id), Country FROM Customer GROUP BY Country ORDER BY COUNT(Id) DESC",
                    "SELECT COUNT(*),Country FROM Customer GROUP BY Country ORDER BY COUNT(Id) DESC",
                    "select `mf:freetext_underscore` as underscoreField, count(*) as numFound from alfresco where `cm:content` = 'world' group by `mf:freetext_underscore` order by numFound asc",
                    // Same queries with some whitespace chars
                    "SELECT    column_names    \n\n FROM table_name WHERE ID=1 ORDER BY column_names",
                    "SELECT \n\n\rCompanyName, \n\nContactName, \tCity, Country FROM Supplier ORDER BY CompanyName asc",
                    "SELECT \n\t\fCompanyName,     ContactName, \t\tCity, \n\nCountry FROM Supplier ORDER BY CompanyName asc, Country desc, ContactName asc",
                    "SELECT \f\f\nCOUNT(Id), \f\tCountry FROM Customer GROUP BY Country ORDER BY COUNT(Id) DESC",
                    "SELECT \n\n\tCOUNT(*),  \t\tCountry FROM Customer GROUP BY Country ORDER BY COUNT(Id) DESC");

    @Test
    public void isSelectStar() {
        Stream.concat(selectStarQueries.stream(), selectStarQueriesWithOrderBy.stream())
                .forEach(this::assertItIsSelectStar);
    }

    @Test
    public void isNotSelectStar() {
        Stream.concat(nonSelectStarQueries.stream(), nonSelectStarQueriesWithOrderBy.stream())
            .forEach(this::assertItIsNotSelectStar);
    }

    @Test
    public void extractSelectStatementFromSelectNode() {
        nonSelectStarQueries.stream()
                .map(this::parseAndCreateSqlStructure)
                .forEach(node -> assertTrue(node.toString(), SqlUtil.extractSelectStatement(node).isPresent()));
    }

    @Test
    public void extractSelectStatementFromOrderByNode()
    {
        nonSelectStarQueriesWithOrderBy.stream()
                    .map(this::parseAndCreateSqlStructure)
                    .forEach(node -> assertTrue(node.toString(), SqlUtil.extractSelectStatement(node).isPresent()));
    }

    @Test
    public void extractSelectStatementWhenNodeIsNotSelectOrOrderByNode()
    {
        List<String> nonSelectQueries =
                asList(
                        "DELETE from table_name WHERE ID <> 13",
                        "UPDATE table_name SET column_name = 'value' WHERE ID=1 OR NAME = 'pippo'",
                        "INSERT INTO Customer (FirstName, LastName, City, Country, Phone) VALUES ('Craig', 'Smith', 'New York', 'USA', '1-01-993 2800')",
                        "INSERT INTO table_name (column_names) SELECT column_names FROM table_name WHERE NAME NOT IN ('Andrea', 'Giorgio')",
                        "INSERT INTO customer (name, surname, city, country, phone_number) " +
                                "SELECT (contact_name, CHARINDEX(' ',contact_name) - 1), " +
                                "       SUBSTRING(contact_name, CHARINDEX(' ', contact_name) + 1, 100), " +
                                "       city, country, phone_number" +
                                "  FROM table_suppliers " +
                                " WHERE company = 'Acme Inc.'");

        nonSelectQueries.stream()
                .map(this::parseAndCreateSqlStructure)
                .forEach(node -> assertFalse(SqlUtil.extractSelectStatement(node).isPresent()));
    }

    @Test
    public void testPredicateExtractor()
    {
        Set<String> predicates = SqlUtil.extractPredicates("select * from  alfresco");
        Assert.assertEquals(Collections.emptySet(), predicates);
        predicates = SqlUtil.extractPredicates("SELECT * FROM  ALFRESCO");
        Assert.assertEquals(Collections.emptySet(), predicates);
        predicates = SqlUtil.extractPredicates("");
        Assert.assertEquals(Collections.emptySet(), predicates);
        predicates = SqlUtil.extractPredicates("select * from  alfresco where `mmm:genre`= 'rock'");
        Assert.assertTrue("mmm:genre", predicates.contains("mmm:genre"));
        Assert.assertEquals(1, predicates.size());
        // check where keyword
        predicates = SqlUtil.extractPredicates("select * from  alfresco where `mmm:wheregenre`= 'rock'");
        Assert.assertTrue("mmm:wheregenre", predicates.contains("mmm:wheregenre"));
        Assert.assertEquals(1, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco WHERE `mmm:genre`= 'rock'");
        Assert.assertTrue("mmm:genre", predicates.contains("mmm:genre"));
        Assert.assertEquals(1, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where `mmm:genre`= 'rock'");
        Assert.assertTrue("mmm:genre", predicates.contains("mmm:genre"));
        Assert.assertEquals(1, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where cm_content = 'rock'");
        Assert.assertTrue("cm_content", predicates.contains("cm_content"));
        Assert.assertEquals(1, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where DBID = '2'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertEquals(1, predicates.size());
        //Handle invalid predicate.
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where 'DBID' = '2'");
        Assert.assertEquals(0, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where DBID = '2' AND PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where DBID = '2' and PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where DBID = '2' And PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where DBID = '2' OR PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where DBID = '2' or PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = SqlUtil.extractPredicates("select * from  alfresco Where DBID = '2' Or PATH = 'Test'");
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertTrue("PATH", predicates.contains("PATH"));
        Assert.assertEquals(2, predicates.size());
        predicates = SqlUtil.extractPredicates("select DBID from alfresco where TYPE = 'content' AND NOT TYPE = 'fm:post' AND DBID = 582");
        Assert.assertTrue("TYPE", predicates.contains("TYPE"));
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertEquals(2, predicates.size());
        predicates = SqlUtil.extractPredicates("select DBID from alfresco where TYPE = 'content' AND not TYPE = 'fm:post' AND DBID = 582");
        Assert.assertTrue("TYPE", predicates.contains("TYPE"));
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertEquals(2, predicates.size());
        predicates = SqlUtil.extractPredicates("select DBID from alfresco where TYPE = 'content' AND Not TYPE = 'fm:post' AND DBID = 582");
        Assert.assertTrue("TYPE", predicates.contains("TYPE"));
        Assert.assertTrue("DBID", predicates.contains("DBID"));
        Assert.assertEquals(2, predicates.size());
    }

    @Test
    public void predicateExtraction_withWhitespaceCharsInQuery()
    {
        for (char w : " \n\r\t\f".toCharArray())
        {
            Assert.assertEquals(
                    asSet("expense:id"),
                    SqlUtil.extractPredicates("select * from alfresco where `expense:id` in (10, 30, 0)".replace(" " , w + "" + w)));

            Assert.assertEquals(
                    asSet("expense:id"),
                    SqlUtil.extractPredicates("select * from alfresco where expense:id in (10, 30, 0)".replace(" " , w + "" + w)));

            Assert.assertEquals(
                    asSet("expense:id", "expense:Location"),
                    SqlUtil.extractPredicates("select * from alfresco where `expense:id` in (10, 30, 0) AND expense:Location='London'".replace(' ' , w)));

            Assert.assertEquals(
                    asSet("expense:id", "expense:Location"),
                    SqlUtil.extractPredicates("select * from alfresco where `expense:id` in (10, 30, 0) AND `expense:Location` = 'London'".replace(" " , w + "" + w)));
        }
    }

    @Test
    public void predicateExtraction_singlePredicateStrictInequalityOperand_shouldExtractCorrectFieldName()
    {
        Set<String> predicates = SqlUtil.extractPredicates("select * from alfresco where customField1 != 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = SqlUtil.extractPredicates("select * from alfresco where customField1 <> 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = SqlUtil.extractPredicates("select * from alfresco where customField1 ~= 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = SqlUtil.extractPredicates("select * from alfresco where customField1 < 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = SqlUtil.extractPredicates("select * from alfresco where customField1 > 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());
    }

    @Test
    public void predicateExtraction_multiPredicateStrictInequalityOperand_shouldExtractCorrectFieldNames()
    {
        Set<String> predicates = SqlUtil.extractPredicates(
                "select * from alfresco where customField1 != 3 AND customField2 <> 3 AND customField3 ~= 3 AND customField4 > 3 AND customField5 < 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertTrue("customField2", predicates.contains("customField2"));
        Assert.assertTrue("customField3", predicates.contains("customField3"));
        Assert.assertTrue("customField4", predicates.contains("customField4"));
        Assert.assertTrue("customField5", predicates.contains("customField5"));

        Assert.assertEquals(5, predicates.size());
    }

    @Test
    public void predicateExtraction_singlePredicateInequalityOperand_shouldExtractCorrectFieldName()
    {
        Set<String> predicates = SqlUtil.extractPredicates("select * from alfresco where customField1 >= 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = SqlUtil.extractPredicates("select * from alfresco where customField1 <= 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());
    }

    @Test
    public void predicateExtraction_multiPredicateInequalityOperand_shouldExtractCorrectFieldNames()
    {
        Set<String> predicates = SqlUtil.extractPredicates(
                "select * from alfresco where customField1 >= 3 AND customField2 <= 3");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertTrue("customField2", predicates.contains("customField2"));

        Assert.assertEquals(2, predicates.size());
    }

    @Test
    public void predicateExtraction_singlePredicateBelongOperand_shouldExtractCorrectFieldNames()
    {
        Set<String> predicates = SqlUtil.extractPredicates(
                "select * from alfresco where customField1 in (3,4,5)");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());

        predicates = SqlUtil.extractPredicates(
                "select * from alfresco where customField1 not in (3,4,5)");
        Assert.assertTrue("customField1", predicates.contains("customField1"));
        Assert.assertEquals(1, predicates.size());
    }

    @Test
    public void predicateExtraction_multiPredicateBelongOperand_shouldExtractCorrectFieldNames()
    {

        // chech event if In and Not in field name may cause problems.
        Set<String> predicates = SqlUtil.extractPredicates(
                "select * from alfresco where customInField1 in (3) AND customNotInField2 not in ('London', 'Paris')");
        Assert.assertTrue("customInField1", predicates.contains("customInField1"));
        Assert.assertTrue("customNotInField2", predicates.contains("customNotInField2"));

        Assert.assertEquals(2, predicates.size());
    }

    private SqlNode parseAndCreateSqlStructure(String query)
    {
        try
        {
            return SqlUtil.parser(query).parseQuery();
        } catch (Exception exception) {
            throw new IllegalArgumentException(exception);
        }
    }

    private void assertItIsSelectStar(String query) {
        try {
            assertTrue("Query >" + query + "< should be marked as a SELECT *", SqlUtil.isSelectStar(query));
        } catch (Exception exception) {
            throw new IllegalArgumentException(exception);
        }
    }

    private void assertItIsNotSelectStar(String query) {
        try {
            assertFalse("Query >" + query + "< shouldn't be marked as a SELECT *", SqlUtil.isSelectStar(query));
        } catch (Exception exception) {
            throw new IllegalArgumentException(exception);
        }
    }
}