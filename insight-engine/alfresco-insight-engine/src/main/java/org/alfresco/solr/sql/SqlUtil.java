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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.solr.handler.AlfrescoSQLHandler;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Optional.of;

/**
 * Booch utility for hosting all SQL queries shared functions.
 *
 * @author Andrea Gazzarini
 * @since 1.1
 */
public abstract class SqlUtil
{
    private final static SqlParser.Config SQL_PARSER_CONFIG =
            SqlParser.configBuilder()
                    .setLex(AlfrescoSQLHandler.CALCITE_LEX_IN_USE)
                    .setConformance(SqlConformanceEnum.ORACLE_12) // allows "bang equal" (!=)
                    .build();

    private final static Predicate<SqlNode> IS_SELECT = node -> node.getKind() == SqlKind.SELECT;
    private final static Predicate<SqlNode> IS_WILD_CARD = node ->
            node.getKind() == SqlKind.IDENTIFIER
                    && "*".equals(node.toSqlString(SqlDialect.DUMMY).toString());

    /**
     * Parses the input query and returns true if it is a select * query.
     *
     * @param sql the SQL query (as a string).
     * @return true if the query is a select * query, false otherwise.
     * @throws SqlParseException in case the query is malformed.
     */
    public static boolean isSelectStar(String sql) throws SqlParseException
    {
        return of(parser(sql).parseQuery())
                .flatMap(SqlUtil::extractSelectStatement)
                .map(SqlSelect::getSelectList)
                .map(SqlNodeList::getList)
                .map(Collection::stream)
                .map(fieldList -> fieldList.anyMatch(IS_WILD_CARD))
                .orElse(false);
    }

    /**
     * Extracts the SELECT statement from a given query.
     * This method is needed because the input Calcite {@link SqlNode} couldn't be directly a {@link SqlSelect}
     * instance; sometimes, when the query contains an ORDER BY clause the top level parsed node is a {@link SqlOrderBy}
     * node which contains the {@link SqlSelect} instance we are looking for.
     *
     * @param query the Calcite top level {@link SqlNode} instance, that is, the result of query parsing.
     * @return the {@link SqlSelect} instance associated with the input query, if exists.
     */
    public static Optional<SqlSelect> extractSelectStatement(SqlNode query)
    {
        switch(query.getKind())
        {
            case SELECT:
                return of(query).map(SqlSelect.class::cast);
            case ORDER_BY:
                return of(query)
                        .map(SqlOrderBy.class::cast)
                        .map(orderBy -> orderBy.query)
                        .filter(IS_SELECT)
                        .map(SqlSelect.class::cast);
            default:
                return Optional.empty();
        }
    }

    /**
     * Creates the Calcite SQL parser.
     * This is done here, in a dedicated method with default visibility because it is internally used by unit tests.
     *
     * @param sql the SQL query.
     * @return a Calcite {@link SqlParser} instance.
     */
    static SqlParser parser(String sql)
    {
        return SqlParser.create(sql, SQL_PARSER_CONFIG);
    }
}