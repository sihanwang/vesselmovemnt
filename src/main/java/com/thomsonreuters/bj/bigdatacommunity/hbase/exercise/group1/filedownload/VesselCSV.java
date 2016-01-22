package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.filedownload;

/**
 Copyright 2005 Bytecode Pty Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * A very simple CSV writer released under a commercial-friendly license.
 *
 * @author Glen Smith
 */
public class VesselCSV implements Closeable, Flushable {

    public static final int INITIAL_STRING_SIZE = 128;
    /**
     * The character used for escaping quotes.
     */
    public static final char DEFAULT_ESCAPE_CHARACTER = '"';
    /**
     * The default separator to use if none is supplied to the constructor.
     */
    public static final char DEFAULT_SEPARATOR = ',';
    /**
     * The default quote character to use if none is supplied to the
     * constructor.
     */
    public static final char DEFAULT_QUOTE_CHARACTER = '"';
    /**
     * The quote constant to use when you wish to suppress all quoting.
     */
    public static final char NO_QUOTE_CHARACTER = '\u0000';
    /**
     * The escape constant to use when you wish to suppress all escaping.
     */
    public static final char NO_ESCAPE_CHARACTER = '\u0000';
    /**
     * Default line terminator uses platform encoding.
     */
    public static final String DEFAULT_LINE_END = "\n";
    private Writer rawWriter;
    private PrintWriter pw;
    private char separator;
    private char quotechar;
    private char escapechar;
    private String lineEnd;
    private ResultSetHelper resultService = new ResultSetHelper();

    /**
     * Constructs CSVWriter using a comma for the separator.
     *
     * @param writer the writer to an underlying CSV source.
     */
    public VesselCSV(Writer writer) {
        this(writer, DEFAULT_SEPARATOR);
    }

    /**
     * Constructs CSVWriter with supplied separator.
     *
     * @param writer    the writer to an underlying CSV source.
     * @param separator the delimiter to use for separating entries.
     */
    public VesselCSV(Writer writer, char separator) {
        this(writer, separator, DEFAULT_QUOTE_CHARACTER);
    }

    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer    the writer to an underlying CSV source.
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     */
    public VesselCSV(Writer writer, char separator, char quotechar) {
        this(writer, separator, quotechar, DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer     the writer to an underlying CSV source.
     * @param separator  the delimiter to use for separating entries
     * @param quotechar  the character to use for quoted elements
     * @param escapechar the character to use for escaping quotechars or escapechars
     */
    public VesselCSV(Writer writer, char separator, char quotechar, char escapechar) {
        this(writer, separator, quotechar, escapechar, DEFAULT_LINE_END);
    }


    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer    the writer to an underlying CSV source.
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     * @param lineEnd   the line feed terminator to use
     */
    public VesselCSV(Writer writer, char separator, char quotechar, String lineEnd) {
        this(writer, separator, quotechar, DEFAULT_ESCAPE_CHARACTER, lineEnd);
    }


    /**
     * Constructs CSVWriter with supplied separator, quote char, escape char and line ending.
     *
     * @param writer     the writer to an underlying CSV source.
     * @param separator  the delimiter to use for separating entries
     * @param quotechar  the character to use for quoted elements
     * @param escapechar the character to use for escaping quotechars or escapechars
     * @param lineEnd    the line feed terminator to use
     */
    public VesselCSV(Writer writer, char separator, char quotechar, char escapechar, String lineEnd) {
        this.rawWriter = writer;
        this.pw = new PrintWriter(writer);
        this.separator = separator;
        this.quotechar = quotechar;
        this.escapechar = escapechar;
        this.lineEnd = lineEnd;
    }

    /**
     * Writes the entire list to a CSV file. The list is assumed to be a
     * String[]
     *
     * @param allLines         a List of String[], with each String[] representing a line of
     *                         the file.
     * @param applyQuotesToAll true if all values are to be quoted.  false if quotes only
     *                         to be applied to values which contain the separator, escape,
     *                         quote or new line characters.
     */
    public void writeAll(List<String[]> allLines, boolean applyQuotesToAll) {
        for (String[] line : allLines) {
            writeNext(line, applyQuotesToAll);
        }
    }

    /**
     * Writes the entire list to a CSV file. The list is assumed to be a
     * String[]
     *
     * @param allLines a List of String[], with each String[] representing a line of
     *                 the file.
     */
    public void writeAll(List<String[]> allLines) {
        for (String[] line : allLines) {
            writeNext(line);
        }
    }

    /**
     * Writes the column names.
     *
     * @param rs - ResultSet containing column names.
     * @throws SQLException - thrown by ResultSet::getColumnNames
     */
    protected void writeColumnNames(ResultSet rs)
            throws SQLException {

        writeNext(resultService.getColumnNames(rs));
    }

    /**
     * Writes the entire ResultSet to a CSV file.
     *
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs                 the result set to write
     * @param includeColumnNames true if you want column names in the output, false otherwise
     * @throws java.io.IOException   thrown by getColumnValue
     * @throws java.sql.SQLException thrown by getColumnValue
     */
    public void writeAll(java.sql.ResultSet rs, boolean includeColumnNames) throws SQLException, IOException {
        writeAll(rs, includeColumnNames, false);
    }

    /**
     * Writes the entire ResultSet to a CSV file.
     *
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs the Result set to write.
     * @param includeColumnNames  include the column names in the output.
     * @param trim remove spaces from the data before writing.
     *
     * @throws java.io.IOException   thrown by getColumnValue
     * @throws java.sql.SQLException thrown by getColumnValue
     */
    public void writeAll(java.sql.ResultSet rs, boolean includeColumnNames, boolean trim) throws SQLException, IOException {


        if (includeColumnNames) {
            writeColumnNames(rs);
        }

        while (rs.next()) {
            writeNext(resultService.getColumnValues(rs, trim));
        }
    }

    /**
     * Writes the next line to the file.
     *
     * @param nextLine         a string array with each comma-separated element as a separate
     *                         entry.
     * @param applyQuotesToAll true if all values are to be quoted.  false applies quotes only
     *                         to values which contain the separator, escape, quote or new line characters.
     */
    public void writeNext(String[] nextLine, boolean applyQuotesToAll) {

        if (nextLine == null) {
            return;
        }

        StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
        for (int i = 0; i < nextLine.length; i++) {

            if (i != 0) {
                sb.append(separator);
            }

            String nextElement = nextLine[i];

            if (nextElement == null) {
                continue;
            }

            Boolean stringContainsSpecialCharacters = stringContainsSpecialCharacters(nextElement);

            if ((applyQuotesToAll || stringContainsSpecialCharacters) && quotechar != NO_QUOTE_CHARACTER) {
                sb.append(quotechar);
            }

            if (stringContainsSpecialCharacters) {
                sb.append(processLine(nextElement));
            } else {
                sb.append(nextElement);
            }

            if ((applyQuotesToAll || stringContainsSpecialCharacters) && quotechar != NO_QUOTE_CHARACTER) {
                sb.append(quotechar);
            }
        }

        sb.append(lineEnd);
        pw.write(sb.toString());
    }

    /**
     * Writes the next line to the file.
     *
     * @param nextLine a string array with each comma-separated element as a separate
     *                 entry.
     */
    public void writeNext(String[] nextLine) {
        writeNext(nextLine, true);
    }

    /**
     * checks to see if the line contains special characters.
     * @param line - element of data to check for special characters.
     * @return true if the line contains the quote, escape, separator, newline or return.
     */
    private boolean stringContainsSpecialCharacters(String line) {
        return line.indexOf(quotechar) != -1 || line.indexOf(escapechar) != -1 || line.indexOf(separator) != -1 || line.contains(DEFAULT_LINE_END) || line.contains("\r");
    }

    /**
     * Processes all the characters in a line.
     * @param nextElement - element to process.
     * @return a StringBuilder with the elements data.
     */
    protected StringBuilder processLine(String nextElement) {
        StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
        for (int j = 0; j < nextElement.length(); j++) {
            char nextChar = nextElement.charAt(j);
            processCharacter(sb, nextChar);
        }

        return sb;
    }

    /**
     * Appends the character to the StringBuilder adding the escape character if needed.
     * @param sb - StringBuffer holding the processed character.
     * @param nextChar - character to process
     */
    private void processCharacter(StringBuilder sb, char nextChar) {
        if (escapechar != NO_ESCAPE_CHARACTER && (nextChar == quotechar || nextChar == escapechar)) {
            sb.append(escapechar).append(nextChar);
        } else {
            sb.append(nextChar);
        }
    }

    /**
     * Flush underlying stream to writer.
     *
     * @throws IOException if bad things happen
     */
    public void flush() throws IOException {

        pw.flush();

    }

    /**
     * Close the underlying stream writer flushing any buffered content.
     *
     * @throws IOException if bad things happen
     */
    public void close() throws IOException {
        flush();
        pw.close();
        rawWriter.close();
    }

    /**
     * Checks to see if the there has been an error in the printstream.
     *
     * @return <code>true</code> if the print stream has encountered an error,
     *          either on the underlying output stream or during a format
     *          conversion.
     */
    public boolean checkError() {
        return pw.checkError();
    }

    /**
     * Sets the result service.
     * @param resultService - the ResultSetHelper
     */
    public void setResultService(ResultSetHelper resultService) {
        this.resultService = resultService;
    }

    /**
     * flushes the writer without throwing any exceptions.
     */
    public void flushQuietly() {
        try {
            flush();
        } catch (IOException e) {
            // catch exception and ignore.
        }
    }
}

class ResultSetHelper {
    public static final int CLOBBUFFERSIZE = 2048;

    // note: we want to maintain compatibility with Java 5 VM's
    // These types don't exist in Java 5
    static final int NVARCHAR = -9;
    static final int NCHAR = -15;
    static final int LONGNVARCHAR = -16;
    static final int NCLOB = 2011;

    static final String DEFAULT_DATE_FORMAT = "dd-MMM-yyyy";
    static final String DEFAULT_TIMESTAMP_FORMAT = "dd-MMM-yyyy HH:mm:ss";

    /**
     * Default Constructor.
     */
    public ResultSetHelper() {
    }

    private static String read(Clob c) throws SQLException, IOException {
        StringBuilder sb = new StringBuilder((int) c.length());
        Reader r = c.getCharacterStream();
        char[] cbuf = new char[CLOBBUFFERSIZE];
        int n;
        while ((n = r.read(cbuf, 0, cbuf.length)) != -1) {
            sb.append(cbuf, 0, n);
        }
        return sb.toString();
    }

    /**
     * Returns the column names from the result set.
     * @param rs - ResultSet
     * @return - a string array containing the column names.
     * @throws SQLException - thrown by the result set.
     */
    public String[] getColumnNames(ResultSet rs) throws SQLException {
        List<String> names = new ArrayList<>();
        ResultSetMetaData metadata = rs.getMetaData();

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            names.add(metadata.getColumnLabel(i + 1));
        }

        String[] nameArray = new String[names.size()];
        return names.toArray(nameArray);
    }

    /**
     * Get all the column values from the result set.
     * @param rs - the ResultSet containing the values.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException - thrown by the result set.
     */
    public String[] getColumnValues(ResultSet rs) throws SQLException, IOException {
        return this.getColumnValues(rs, false, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    }

    /**
     * Get all the column values from the result set.
     * @param rs - the ResultSet containing the values.
     * @param trim - values should have white spaces trimmed.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException - thrown by the result set.
     */
    public String[] getColumnValues(ResultSet rs, boolean trim) throws SQLException, IOException {
        return this.getColumnValues(rs, trim, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    }

    /**
     * Get all the column values from the result set.
     * @param rs - the ResultSet containing the values.
     * @param trim - values should have white spaces trimmed.
     * @param dateFormatString - format String for dates.
     * @param timeFormatString - format String for timestamps.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException - thrown by the result set.
     */
    public String[] getColumnValues(ResultSet rs, boolean trim, String dateFormatString, String timeFormatString) throws SQLException, IOException {
        List<String> values = new ArrayList<>();
        ResultSetMetaData metadata = rs.getMetaData();

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            values.add(getColumnValue(rs, metadata.getColumnType(i + 1), i + 1, trim, dateFormatString, timeFormatString));
        }

        String[] valueArray = new String[values.size()];
        return values.toArray(valueArray);
    }

    /**
     * changes an object to a String.
     * @param obj - Object to format.
     * @return - String value of an object or empty string if the object is null.
     */
    protected String handleObject(Object obj) {
        return obj == null ? "" : String.valueOf(obj);
    }

    /**
     * changes a BigDecimal to String.
     * @param decimal - BigDecimal to format
     * @return String representation of a BigDecimal or empty string if null
     */
    protected String handleBigDecimal(BigDecimal decimal) {
        return decimal == null ? "" : decimal.toString();
    }

    /**
     * Retrieves the string representation of an Long value from the result set.
     * @param rs - Result set containing the data.
     * @param columnIndex - index to the column of the long.
     * @return - the string representation of the long
     * @throws SQLException - thrown by the result set on error.
     */
    protected String handleLong(ResultSet rs, int columnIndex) throws SQLException {
        long lv = rs.getLong(columnIndex);
        return rs.wasNull() ? "" : Long.toString(lv);
    }

    /**
     * Retrieves the string representation of an Integer value from the result set.
     * @param rs - Result set containing the data.
     * @param columnIndex - index to the column of the integer.
     * @return - string representation of the Integer.
     * @throws SQLException - returned from the result set on error.
     */
    protected String handleInteger(ResultSet rs, int columnIndex) throws SQLException {
        int i = rs.getInt(columnIndex);
        return rs.wasNull() ? "" : Integer.toString(i);
    }

    /**
     * Retrieves a date from the result set.
     * @param rs - Result set containing the data
     * @param columnIndex - index to the column of the date
     * @param dateFormatString - format for the date
     * @return - formatted date.
     * @throws SQLException - returned from the result set on error.
     */
    protected String handleDate(ResultSet rs, int columnIndex, String dateFormatString) throws SQLException {
        java.sql.Date date = rs.getDate(columnIndex);
        String value = null;
        if (date != null) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);
            value = dateFormat.format(date);
        }
        return value;
    }

    /**
     * Return time read from ResultSet.
     *
     * @param time time read from ResultSet
     * @return String version of time or null if time is null.
     */
    protected String handleTime(Time time) {
        return time == null ? null : time.toString();
    }

    /**
     * The formatted timestamp.
     * @param timestamp - timestamp read from resultset
     * @param timestampFormatString - format string
     * @return - formatted time stamp.
     */
    protected String handleTimestamp(Timestamp timestamp, String timestampFormatString) {
        SimpleDateFormat timeFormat = new SimpleDateFormat(timestampFormatString);
        return timestamp == null ? null : timeFormat.format(timestamp);
    }

    private String getColumnValue(ResultSet rs, int colType, int colIndex, boolean trim, String dateFormatString, String timestampFormatString)
            throws SQLException, IOException {

        String value = "";

        switch (colType) {
            case Types.BIT:
            case Types.JAVA_OBJECT:
                value = handleObject(rs.getObject(colIndex));
                break;
            case Types.BOOLEAN:
                boolean b = rs.getBoolean(colIndex);
                value = Boolean.valueOf(b).toString();
                break;
            case NCLOB: // todo : use rs.getNClob
            case Types.CLOB:
                Clob c = rs.getClob(colIndex);
                if (c != null) {
                    value = read(c);
                }
                break;
            case Types.BIGINT:
                value = handleLong(rs, colIndex);
                break;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
            case Types.NUMERIC:
                value = handleBigDecimal(rs.getBigDecimal(colIndex));
                break;
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                value = handleInteger(rs, colIndex);
                break;
            case Types.DATE:
                value = handleDate(rs, colIndex, dateFormatString);
                break;
            case Types.TIME:
                value = handleTime(rs.getTime(colIndex));
                break;
            case Types.TIMESTAMP:
                value = handleTimestamp(rs.getTimestamp(colIndex), timestampFormatString);
                break;
            case NVARCHAR: // todo : use rs.getNString
            case NCHAR: // todo : use rs.getNString
            case LONGNVARCHAR: // todo : use rs.getNString
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
            case Types.CHAR:
                String columnValue = rs.getString(colIndex);
                if (trim && columnValue != null) {
                    value = columnValue.trim();
                } else {
                    value = columnValue;
                }
                break;
            default:
                value = "";
        }


        if (value == null) {
            value = "";
        }

        return value;
    }
}