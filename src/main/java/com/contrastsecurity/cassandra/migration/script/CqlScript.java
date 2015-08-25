/**
 * Copyright 2010-2015 Axel Fontaine
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.contrastsecurity.cassandra.migration.script;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.utils.StringUtils;
import com.contrastsecurity.cassandra.migration.utils.scanner.Resource;
import com.datastax.driver.core.Session;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Cql script containing a series of statements terminated by a delimiter (eg: ;).
 * Single-line (--) and multi-line (/* * /) comments are stripped and ignored.
 */
public class CqlScript {
    private static final Log LOG = LogFactory.getLog(CqlScript.class);

    /**
     * The cql statements contained in this script.
     */
    private final List<String> cqlStatements;

    /**
     * The resource containing the statements.
     */
    private final Resource resource;

    /**
     * Creates a new cql script from this source.
     *
     * @param cqlScriptSource The cql script as a text block with all placeholders already replaced.
     */
    public CqlScript(String cqlScriptSource) {
        this.cqlStatements = parse(cqlScriptSource);
        this.resource = null;
    }

    /**
     * Creates a new cql script from this resource.
     *
     * @param cqlScriptResource The resource containing the statements.
     * @param encoding          The encoding to use.
     */
    public CqlScript(Resource cqlScriptResource, String encoding) {
        String cqlScriptSource = cqlScriptResource.loadAsString(encoding);
        this.cqlStatements = parse(cqlScriptSource);

        this.resource = cqlScriptResource;
    }

    /**
     * For increased testability.
     *
     * @return The cql statements contained in this script.
     */
    public List<String> getCqlStatements() {
        return cqlStatements;
    }

    /**
     * @return The resource containing the statements.
     */
    public Resource getResource() {
        return resource;
    }

    /**
     * Executes this script against the database.
     * @param session Cassandra session
     */
    public void execute(final Session session) {
        for (String cqlStatement : cqlStatements) {
            LOG.debug("Executing CQL: " + cqlStatement);
            session.execute(cqlStatement);
        }
    }

    /**
     * Parses this script source into statements.
     *
     * @param cqlScriptSource The script source to parse.
     * @return The parsed statements.
     */
    /* private -> for testing */
    List<String> parse(String cqlScriptSource) {
        return linesToStatements(readLines(new StringReader(cqlScriptSource)));
    }

    /**
     * Turns these lines in a series of statements.
     *
     * @param lines The lines to analyse.
     * @return The statements contained in these lines (in order).
     */
    /* private -> for testing */
    List<String> linesToStatements(List<String> lines) {
        List<String> statements = new ArrayList<>();

        Delimiter nonStandardDelimiter = null;
        CqlStatementBuilder cqlStatementBuilder = new CqlStatementBuilder();
        for (int lineNumber = 1; lineNumber <= lines.size(); lineNumber++) {
            String line = lines.get(lineNumber - 1);

            if (cqlStatementBuilder.isEmpty()) {
                if (!StringUtils.hasText(line)) {
                    // Skip empty line between statements.
                    continue;
                }

                Delimiter newDelimiter = cqlStatementBuilder.extractNewDelimiterFromLine(line);
                if (newDelimiter != null) {
                    nonStandardDelimiter = newDelimiter;
                    // Skip this line as it was an explicit delimiter change directive outside of any statements.
                    continue;
                }

                cqlStatementBuilder.setLineNumber(lineNumber);

                // Start a new statement, marking it with this line number.
                if (nonStandardDelimiter != null) {
                    cqlStatementBuilder.setDelimiter(nonStandardDelimiter);
                }
            }

            cqlStatementBuilder.addLine(line);

            if (cqlStatementBuilder.isTerminated()) {
                String cqlStatement = cqlStatementBuilder.getCqlStatement();
                statements.add(cqlStatement);
                LOG.debug("Found statement: " + cqlStatement);

                cqlStatementBuilder = new CqlStatementBuilder();
            } else if (cqlStatementBuilder.canDiscard()) {
                cqlStatementBuilder = new CqlStatementBuilder();
            }
        }

        // Catch any statements not followed by delimiter.
        if (!cqlStatementBuilder.isEmpty()) {
            statements.add(cqlStatementBuilder.getCqlStatement());
        }

        return statements;
    }

    /**
     * Parses the textual data provided by this reader into a list of lines.
     *
     * @param reader The reader for the textual data.
     * @return The list of lines (in order).
     * @throws IllegalStateException Thrown when the textual data parsing failed.
     */
    private List<String> readLines(Reader reader) {
        List<String> lines = new ArrayList<String>();

        BufferedReader bufferedReader = new BufferedReader(reader);
        String line;

        try {
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            String message = resource == null ?
                    "Unable to parse lines" :
                    "Unable to parse " + resource.getLocation() + " (" + resource.getLocationOnDisk() + ")";
            throw new CassandraMigrationException(message, e);
        }

        return lines;
    }
}
