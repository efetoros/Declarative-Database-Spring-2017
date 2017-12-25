package db;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.LinkedHashSet;
import java.util.HashMap;

public class Database {
    LinkedHashMap<String, Table> tables;

    public boolean isBoggleWord(String word) {
        if (word.length() < 3 || word.length() > 3) {
            return false;
        }
        return true;
    }

    public Database() {
        tables = new LinkedHashMap<String, Table>();
    }

    private String load(String tablename) throws IOException {
        try {
            BufferedReader buff = new BufferedReader(new FileReader(tablename));
            try {
                String columnames = buff.readLine();
                String tableName = tablename.substring(0, tablename.length() - 4);
                String createCommand = tableName + " (" + columnames + ")";
                createTable(createCommand);
                try {
                    for (String nexRow = buff.readLine();
                         nexRow != null; nexRow = buff.readLine()) {
                        nexRow = nexRow.replaceAll("'", "");
                        String insertCommand = tableName + " values " + nexRow;
                        insertInto(insertCommand);
                    }
                } catch (IOException ex) {
                    return "ERROR: error";
                }
            } catch (IOException ex) {
                return "ERROR: error";
            }
        } catch (FileNotFoundException ex) {
            return "ERROR: error";
        }
        return "";
    }

    public String store(String tableName) {
        if (!tables.containsKey(tableName)) {
            return "ERROR: table doesn't exist";
        }
        Table table1 = tables.get(tableName);
        Iterator columnNamesIterator = table1.columnNames.iterator();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(tableName + ".tbl"));
            for (int i = 0; i < table1.columnNames.size(); i++) {
                String temp = (String) columnNamesIterator.next();
                writer.write(temp);
                if (i != table1.columnNames.size() - 1) {
                    writer.write(",");
                }
            }
            for (Row row : table1.rows.values()) {
                Iterator columnsIterator = row.columns.values().iterator();
                Iterator columnNamesIterator2 = table1.columnNames.iterator();
                writer.newLine();
                while (columnsIterator.hasNext()) {
                    String currColName = (String) columnNamesIterator2.next();
                    if (currColName.split(" ")[1].equals("string")) {
                        writer.write("'" + columnsIterator.next() + "'");
                    } else {
                        writer.write(String.valueOf(columnsIterator.next()));
                    }
                    if (columnsIterator.hasNext()) {
                        writer.write(",");
                    }

                }
            }

            writer.close();
        } catch (IOException ex) {
            return "ERROR: ";
        }

        return "";
    }

    public String drop(String tableName) {
        if (!tables.keySet().contains(tableName)) {
            return ("ERROR: no such key exists");
        }
        tables.remove(tableName);

        return "";
    }

    private String createTable(String expr) {
        try {
            Matcher m;
            if ((m = CREATE_NEW.matcher(expr)).matches()) {
                createNewTable(m.group(1), m.group(2).split(COMMA));
            } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
                createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4));
            } else {
                return "ERROR: Malformed create";
            }
            return "";
        } catch (RuntimeException ex) {
            return "ERROR: can't create table";
        }
    }

    private void createNewTable(String tableName, String[] cols) throws RuntimeException {
        LinkedList<String> columnNames = createColumnNames(cols);
        Table newTable = new Table(columnNames);
        tables.put(tableName, newTable);
    }

    private LinkedList<String> createColumnNames(String[] cols) throws RuntimeException {
        LinkedList<String> columnNames = new LinkedList<>();
        for (int i = 0; i < cols.length; i++) {
            String columnName = cols[i];
            columnName = columnName.trim().replaceAll(" +", " ");
            String[] columnNameType = columnName.split(" ");
            if (columnNameType.length < 2 || (!columnNameType[1].equals("string")
                    && !columnNameType[1].equals("int")
                    && !columnNameType[1].equals("float"))) {
                throw new RuntimeException();
            }
            columnNames.add(columnName);
        }
        return columnNames;
    }

    private Table returnNewTable(LinkedList<String> cols) {
        Table newTable = new Table(cols);
        return newTable;
    }

    private void createSelectedTable(String name, String exprs,
                                     String tablesChosen, String conds) throws RuntimeException {
        Table createdTable = select(exprs, tablesChosen, conds);
        tables.put(name, createdTable);
    }

    private String insertInto(String expr) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            return "ERROR: Malformed insert";
        }
        try {
            insertIntoTable(m.group(1), m.group(2).split(COMMA));
            return "";
        } catch (RuntimeException ex) {
            return "ERROR: error";
        }
    }

    private void insertIntoTable(String tableName, String[] values) throws RuntimeException {
        try {
            Table table1 = tables.get(tableName);
            LinkedList<String> columnNames = table1.columnNames;
            Row rowGuy = new Row();
            Iterator cnIt = columnNames.iterator();
            int i = 0; /* make sure the number of values and columns is equal */
            while (cnIt.hasNext()) {
                if (itemType(values[i]).equals("float")) {
                    Float float1 = new Float(values[i]);
                    String stringFloat = String.format("%.3f", float1);
                    values[i] = stringFloat;
                }
                rowGuy.add((String) cnIt.next(), values[i].replaceAll("'", ""));
                i++;
            }
            table1.add(Integer.toString(table1.size()), rowGuy);
            table1.increaseSize();
        } catch (RuntimeException ex) {
            throw new RuntimeException();
        }
    }

    private void insertIntoFromJoin(Table table, String[] values) {
        Table table1 = table;
        LinkedList<String> columnNames = table1.columnNames;
        Row rowGuy = new Row();
        Iterator cnIt = columnNames.iterator();
        int i = 0; /* make sure the number of values and columns is equal */
        while (cnIt.hasNext()) {
            if (itemType(values[i]).equals("float")) {
                Float float1 = new Float(values[i]);
                String stringFloat = String.format("%.3f", float1);
                values[i] = stringFloat;
            }
            rowGuy.add((String) cnIt.next(), values[i].replaceAll("'", ""));
            i++;
        }
        table1.add(Integer.toString(table1.size()), rowGuy);
        table1.increaseSize();
    }

    private String select(String expr) {
        try {
            Matcher m = SELECT_CLS.matcher(expr);
            if (!m.matches()) {
                return "ERROR: Malformed select";
            }
            return print(select(m.group(1), m.group(2), m.group(3)));
        } catch (RuntimeException ex) {
            return "ERROR: WRONG TYPES";
        }
    }

    private Table select(String exprs, String tablesChosen, String conds) throws RuntimeException {
        tablesChosen = tablesChosen.trim().replaceAll(" +", " ");
        String[] tablesParts = tablesChosen.split(COMMA);
        String[] exprsParts = exprs.split(COMMA);
        Table returnTable;
        if (tablesParts.length > 2) {
            Table t3 = recursiveJoin(tablesParts);
            returnTable = multiColJoin(t3, exprsParts);
        } else if (tablesParts.length == 2) {
            Table table1 = tables.get(tablesParts[0]);
            Table table2 = tables.get(tablesParts[1]);
            Table t3 = select2Tables(table1, table2);
            returnTable = multiColJoin(t3, exprsParts);
        } else {
            Table t3 = recursiveJoin(tablesParts);
            returnTable = multiColJoin(t3, exprsParts);
        }
        if (conds != null) {
            String cdss = conds.trim().replaceAll(" +", " ");
            String[] cds = cdss.split(" and ");

            for (int i = 0; i < cds.length; i++) {
                String[] words = cds[i].split(" ");
                returnTable = where(returnTable, words[0], words[1], words[2]);
            }
        }
        return returnTable;
    }

    private Table recursiveJoin(String[] tablesParts) {
        if (tablesParts.length == 1) {
            return tables.get(tablesParts[0]);
        } else if (tablesParts.length == 2) {
            Table table1 = tables.get(tablesParts[0]);
            Table table2 = tables.get(tablesParts[1]);
            return select2Tables(table1, table2);
        } else {
            return select2Tables(recursiveJoin(
                    Arrays.copyOfRange(tablesParts, 0, tablesParts.length / 2)),
                    recursiveJoin(
                            Arrays.copyOfRange(tablesParts,
                                    tablesParts.length / 2, tablesParts.length)));
        }
    }

    private Table select2Tables(Table table1, Table table2) {
        LinkedHashMap<String, LinkedList> matches = new LinkedHashMap<>();

        LinkedList t1ColNames = table1.columnNames;
        LinkedList t2ColNames = table2.columnNames;
        Iterator t1ColNamesIt = t1ColNames.iterator();
        LinkedList sharedColumns = new LinkedList();

        while (t1ColNamesIt.hasNext()) {
            String t1ColName = (String) t1ColNamesIt.next();
            Iterator t2ColNamesIt = t2ColNames.iterator();
            while (t2ColNamesIt.hasNext()) {
                String t2ColName = (String) t2ColNamesIt.next();
                if (t1ColName.equals(t2ColName)) {
                    sharedColumns.add(t1ColName);
                }
            }
        }

        Table t3;
        if (sharedColumns.size() > 0) {
            findMatches(matches, table1, table2, sharedColumns);
            LinkedList<String> allUniqueCols = uniqueColumns(sharedColumns, table1, table2);
            t3 = returnNewTable(allUniqueCols);
            insertFromMatchesIntoTable(t3, matches, table1, table2, sharedColumns);
            return t3;
        } else {
            return cartesianJoin(table1, table2);
        }
    }

    private Table combine2Tables(Table table1, Table table2) {
        LinkedList t1ColumnNames = table1.columnNames;
        LinkedList t2ColumnNames = table2.columnNames;
        LinkedList t3ColumnNames = new LinkedList();
        for (int i = 0; i < t1ColumnNames.size(); i++) {
            t3ColumnNames.add(t1ColumnNames.get(i));
        }
        for (int i = 0; i < t2ColumnNames.size(); i++) {
            t3ColumnNames.add(t2ColumnNames.get(i));
        }
        Table t3 = new Table(t3ColumnNames);
        HashMap<String, Row> t3Rows = t3.rows;
        HashMap<String, Row> t1Rows = table1.rows;
        HashMap<String, Row> t2Rows = table2.rows;
        for (int i = 0; i < table1.rows.size(); i++) {
            Row newRow = new Row();
            Iterator t1ColumnNameIterator = t1ColumnNames.iterator();
            while (t1ColumnNameIterator.hasNext()) {
                String key = (String) t1ColumnNameIterator.next();
                newRow.add(key, t1Rows.get(Integer.toString(i)).columns.get(key));
            }
            Iterator t2ColumnNameIterator = t2ColumnNames.iterator();
            while (t2ColumnNameIterator.hasNext()) {
                String key = (String) t2ColumnNameIterator.next();
                newRow.add(key, t2Rows.get(Integer.toString(i)).columns.get(key));
            }
            t3Rows.put(Integer.toString(i), newRow);
        }
        return t3;
    }

    private Table multiColJoin(Table t3, String[] expressions) throws RuntimeException {
        if (expressions.length == 1) {
            if (expressions[0].equals("*")) {
                return t3;
            }
            String asColName = expressions[0];
            if (expressions[0].contains(" as ")) {
                asColName = expressions[0].split("\\s+as\\s+")[1];
            }
            return exprsParseUpdated(t3, expressions[0], asColName);
        } else {
            return combine2Tables(multiColJoin(t3, Arrays.copyOfRange(expressions,
                    0, expressions.length / 2)),
                    multiColJoin(t3, Arrays.copyOfRange(expressions,
                            expressions.length / 2, expressions.length)));
        }
    }

    private Table exprsParseUpdated(Table t3, String exprs,
                                    String asColName) throws RuntimeException {
        LinkedList colNames = new LinkedList();
        Table finalTable = new Table(colNames);
        if (itemType(exprs).equals("string")) {
            String colType = colType(t3, exprs);
            String nonVal;
            if (colType.equals("string")) {
                nonVal = "";
            } else if (colType.equals("int")) {
                nonVal = "0";
            } else {
                nonVal = "0.000";
            }
            asColName += " " + colType;
            colNames.add(asColName);
            addColumnToTable(finalTable, exprs + " " + colType,
                    combineOneCol(t3, exprs, nonVal, "+", colType, colType));
        } else if (exprs.contains("+") || exprs.contains("-")
                || exprs.contains("*") || exprs.contains("/")) {
            String operator = exprs.replaceAll("([aA-zZ0-9]*_*)+", "");
            String[] pieces = exprs.split("\\s*\\W\\s*");
            operator = operator.replaceAll(" ", "");
            String operand0 = pieces[0];
            String operand1 = pieces[1];
            String op0Type = colType(t3, operand0);
            String op1Type = "";
            if (!tableContainsColumn(t3, operand0)) {
                throw new RuntimeException("ERROR: operand0 does not exist");
            }
            if (op0Type.equals("string")) {
                asColName += " string";
                LinkedList values;
                if (tableContainsColumn(t3, operand1)) {
                    op1Type = colType(t3, operand1);
                    stringCheck(finalTable, t3, operand0,
                            operand1, op0Type, op1Type, operator);
                } else {
                    op1Type = itemType(operand1);
                    stringCheck(finalTable, t3, operand0,
                            operand1, op0Type, op1Type, operator);
                }
                values = combineOneCol(t3,
                        operand0, operand1, operator, op0Type, op1Type);
                addColumnToTable(finalTable, asColName, values);
            } else {
                if (tableContainsColumn(t3, operand1)) {
                    op1Type = colType(t3, operand1);
                    if (op0Type.equals("float") || op1Type.equals("float")) {
                        asColName += " float";
                    } else {
                        asColName += " int";
                    }
                    if (colType(t3, operand1).equals("string")) {
                        throw new RuntimeException("operand1 does not match operand0");
                    }
                    LinkedList values = combineTwoCols(t3, operand0, operand1,
                            operator, op0Type, op1Type);
                    addColumnToTable(finalTable, asColName, values);
                } else {
                    op1Type = itemType(operand1);
                    if (op0Type.equals("float") || op1Type.equals("float")) {
                        asColName += " float";
                    } else {
                        asColName += " int";
                    }
                    if (itemType(operand1).equals("string")) {
                        throw new RuntimeException("operand1 does not match operand0");
                    }
                    LinkedList values = combineOneCol(t3, operand0, operand1,
                            operator, op0Type, op1Type);
                    addColumnToTable(finalTable, asColName, values);
                }
            }
        }
        if (!finalTable.columnNames.contains(asColName) && !asColName.equals("*")) {
            finalTable.columnNames.add(asColName);
        }
        return finalTable;
    }


    private void stringCheck(Table finalTable, Table t3, String operand0,
                             String operand1, String op0Type,
                             String op1Type, String operator) throws RuntimeException {
        if (!operator.equals("+")) {
            throw new RuntimeException("ERROR: operator does not match operand0");
        } else if (tableContainsColumn(t3, operand1)) {
            if (!op1Type.equals("string")) {
                throw new RuntimeException("ERROR: operand1 does not match operand0");
            }
        } else {
            if (!itemType(operand1).equals("string")) {
                throw new RuntimeException("ERROR: operand1 does not match operand0");
            }
        }
    }

    private boolean tableContainsColumn(Table t, String col) {
        LinkedList columnNames = t.columnNames;
        for (int i = 0; i < columnNames.size(); i++) {
            String currCol = (String) columnNames.get(i);
            if (currCol.contains(col)) {
                return true;
            }
        } return false;
    }

    private String colType(Table t, String col) {
        LinkedList columnNames = t.columnNames;
        for (int i = 0; i < columnNames.size(); i++) {
            String currCol = (String) columnNames.get(i);
            if (currCol.contains(col)) {
                return currCol.split("\\s")[1];
            }
        } return "";
    }

    private String itemType(String item) {
        if (item.matches("-?[0-9]*\\.[0-9]+")) {
            return "float";
        } else if (item.matches("-?[0-9]+")) {
            return "int";
        } else if (item.matches("([aA-zZ0-9]*_*)+")) {
            return "string";
        } else {
            return "no match";
        }
    }

    private void addColumnToTable(Table table,
                                  String columnName, LinkedList<String> values) {
        for (int i = 0; i < values.size(); i++) {
            LinkedHashMap<String, String> currCol = new LinkedHashMap<>();
            currCol.put(columnName, values.get(i));
            Row newRow = new Row();
            newRow.columns = currCol;
            table.rows.put(Integer.toString(i), newRow);
        }
    }

    private LinkedList combineTwoCols(Table t3, String operand0, String operand1, String operator,
                                      String op0Type, String op1Type) {
        LinkedHashMap<String, Row> allRows = t3.rows;
        Iterator rowIterator = allRows.keySet().iterator();
        LinkedList newCol = new LinkedList();
        String item1Type = op0Type;
        String item2Type = op1Type;
        while (rowIterator.hasNext()) {
            Row currRow = (Row) allRows.get(rowIterator.next());
            String value1 = currRow.columns.get(operand0 + " " + op0Type);
            String value2 = currRow.columns.get(operand1 + " " + op1Type);
            if (value1.equals("NaN") || value2.equals("NaN")) {
                newCol.add("NaN");
            } else if (item1Type.equals("int") && item2Type.equals("int")) {
                int newVal1 = Integer.parseInt(value1);
                int newVal2 = Integer.parseInt(value2);
                String valToAdd = Integer.toString(operateOnValue(newVal1, newVal2, operator));
                newCol.add(valToAdd);
            } else if (item1Type.equals("float") || item2Type.equals("float")) {
                Float float1 = new Float(value1);
                Float float2 = new Float(value2);
                Float valToAdd = operateOnValue(float1, float2, operator);
                String stringFloat = String.format("%.3f", valToAdd);
                newCol.add(stringFloat);
            } else {
                String newVal1 = value1;
                String newVal2 = value2;
                newCol.add(operateOnValue(newVal1, newVal2));
            }
        }
        return newCol;
    }

    private LinkedList combineOneCol(Table t3, String operand0, String operand1, String operator,
                                     String op0Type, String op1Type) {
        LinkedHashMap<String, Row> allRows = t3.rows;
        Iterator rowIterator = allRows.keySet().iterator();
        LinkedList newCol = new LinkedList();
        while (rowIterator.hasNext()) {
            Row newRow = new Row();
            Row currRow = allRows.get(rowIterator.next());
            String value = currRow.columns.get(operand0 + " " + op0Type);
            String itemType = op0Type;
            String literalType = op1Type;
            if (value.equals("NaN") || operand1.equals("NaN")) {
                newCol.add("NaN");
            } else if (value.equals("NOVALUE")) {
                newCol.add(operand1);
            } else if (operand1.equals("NOVALUE")) {
                newCol.add(value);
            } else if (literalType.equals("int") && itemType.equals("int")) {
                int newVal = Integer.parseInt(value);
                int newOp1 = Integer.parseInt(operand1);
                if (newOp1 == 0 && operator.equals("/")) {
                    newCol.add("NaN");
                } else {
                    String valToAdd = Integer.toString(operateOnValue(newVal, newOp1, operator));
                    newCol.add(valToAdd);
                }
            } else if (literalType.equals("float") || itemType.equals("float")) {
                Float float1 = Float.valueOf(value);
                Float float2 = Float.valueOf(operand1);
                if (float2 == 0.000 && operator.equals("/")) {
                    newCol.add("NaN");
                } else {
                    Float valToAdd = operateOnValue(float1, float2, operator);
                    String stringFloat = String.format("%.3f", valToAdd);
                    newCol.add(stringFloat);
                }
            } else {
                String newVal = value;
                String newOp1 = operand1;
                newCol.add(operateOnValue(newVal, newOp1));
            }
        }
        return newCol;
    }

    private String operateOnValue(String operand, String literal) {

        if (operand.equals("NaN") || literal.equals("NaN")) {
            return "NaN";
        } else {
            String newVal = operand + literal;
            return newVal;
        }
    }

    private int operateOnValue(int operand, int literal, String operator) {
        int newVal;
        if (operator.equals("+")) {
            newVal = operand + literal;
        } else if (operator.equals("-")) {
            newVal = operand - literal;
        } else if (operator.equals("*")) {
            newVal = operand * literal;
        } else {
            newVal = operand / literal;
        }
        return newVal;
    }

    private float operateOnValue(float operand, float literal, String operator) {
        float newVal;
        if (operator.equals("+")) {
            newVal = operand + literal;
        } else if (operator.equals("-")) {
            newVal = operand - literal;
        } else if (operator.equals("*")) {
            newVal = operand * literal;
        } else {
            newVal = operand / literal;
        }
        return newVal;
    }

    private void findMatches(LinkedHashMap<String, LinkedList> matches,
                             Table table1, Table table2, LinkedList sharedColumns) {

        // adds T1 rownames to matches hashmap
        Iterator t1RowNames = table1.rows.keySet().iterator();
        while (t1RowNames.hasNext()) {
            matches.put((String) t1RowNames.next(), new LinkedList());
        }

        // checks for matches with each t1rownames in matches and adds
        // to the associated linkedlist if there is a match
        Iterator t2RowNames = table2.rows.keySet().iterator();
        while (t2RowNames.hasNext()) {
            String t2RowName = (String) t2RowNames.next();
            String t2ElementsFromShared = "";
            Row t2Row = table2.rows.get(t2RowName);
            for (int i = 0; i < sharedColumns.size(); i++) {
                String guy = (String) sharedColumns.get(i);
                t2ElementsFromShared += t2Row.columns.get(guy);
                if (i != sharedColumns.size() - 1) {
                    t2ElementsFromShared += " ";
                }
            }

            Iterator matchesKeys = matches.keySet().iterator();
            while (matchesKeys.hasNext()) {
                String matchesPlace =  (String) matchesKeys.next();
                Row t1CurrentRow = table1.rows.get(matchesPlace);
                String t1ElementsFromShared = "";
                for (int i = 0; i < sharedColumns.size(); i++) {
                    String guy = (String) sharedColumns.get(i);
                    t1ElementsFromShared += t1CurrentRow.columns.get(guy);
                    if (i != sharedColumns.size() - 1) {
                        t1ElementsFromShared += " ";
                    }
                }

                if (t1ElementsFromShared.equals(t2ElementsFromShared)) {
                    LinkedList val = matches.get(matchesPlace);
                    val.add(t2RowName);
                    matches.put(matchesPlace, val);
                    break;
                }
            }
        }

    }

    private LinkedList<String> uniqueColumns(LinkedList sharedColumns,
                                             Table table1, Table table2) {
        // creates a LinkedHashSet of all the columns with no duplicates
        // and then returns a  linkedlist
        LinkedHashSet<String> comboCols = new LinkedHashSet<>();
        Iterator t1ColNamesIt = table1.columnNames.iterator();
        Iterator t2ColNamesIt = table2.columnNames.iterator();
        for (int i = 0; i < sharedColumns.size(); i++) {
            comboCols.add((String) sharedColumns.get(i));
        }
        while (t1ColNamesIt.hasNext()) {
            comboCols.add((String) t1ColNamesIt.next());
        }
        while (t2ColNamesIt.hasNext()) {
            comboCols.add((String) t2ColNamesIt.next());
        }

        LinkedList<String> comboCols2 = new LinkedList<>();
        Iterator comboColsIt =  comboCols.iterator();

        while (comboColsIt.hasNext()) {
            comboCols2.add((String) comboColsIt.next());
        }

        return comboCols2;
    }

    private void insertFromMatchesIntoTable(Table t3,
                                            LinkedHashMap<String, LinkedList> matches,
                                            Table table1, Table table2,
                                            LinkedList sharedColumns) {
        //from the matches insert all new rows into a new table
        Iterator matchesKeys = matches.keySet().iterator();
        while (matchesKeys.hasNext()) {
            String t1RowInMatches = (String) matchesKeys.next();
            LinkedList matchesWithT2 = matches.get(t1RowInMatches);
            if (!matchesWithT2.isEmpty()) {
                for (int i = 0; i < matchesWithT2.size(); i++) {
                    String[] rowNames = {t1RowInMatches, (String) matchesWithT2.get(i)};
                    join(sharedColumns, table1, table2, rowNames, t3);
                }
            }
        }
    }

    private void join(LinkedList sharedCols, Table table1,
                      Table table2, String[] rowName, Table t3) {
        Table t1 = table1;
        Table t2 = table2;
        String t1RowName = rowName[0];
        String t2RowName = rowName[1];
        int placeHolder = 0;
        String[] rowvalue = new String[t3.columnNames.size()];
        for (int i = 0; i < sharedCols.size()
                + (t3.columnNames.size() - t2.columnNames.size()); i++) {
            rowvalue[i] = t1.rows.get(t1RowName).columns.get(t3.columnNames.get(i));
            placeHolder++;
        }

        for (int i = placeHolder; i < t3.columnNames.size(); i++) {
            rowvalue[i] = t2.rows.get(t2RowName).columns.get(t3.columnNames.get(i));
        }

        insertIntoFromJoin(t3, rowvalue);
    }

    private Table cartesianJoin(Table t1, Table t2) {
        LinkedList<String> cc = new LinkedList<>();
        cc.addAll(t1.columnNames);
        cc.addAll(t2.columnNames);
        Table finaltable = returnNewTable(cc);
        String[] rowvalues = new String[finaltable.columnNames.size()];
        for (int i = 0; i < t1.size(); i++) {
            for (int y = 0; y < t1.columnNames.size(); y++) {
                rowvalues[y] =
                        t1.rows.get(Integer.toString(i)).
                                columns.get(finaltable.columnNames.get(y));
            }
            for (int x = 0; x < t2.size(); x++) {
                for (int y = t1.columnNames.size(); y < finaltable.columnNames.size(); y++) {
                    rowvalues[y] =
                            t2.rows.get(Integer.toString(x)).
                                    columns.get(finaltable.columnNames.get(y));

                }
                insertIntoFromJoin(finaltable, rowvalues);
            }

        }
        return finaltable;
    }

    private Table where(Table table,
                        String columnName, String comparison,
                        String extra1) throws RuntimeException {
        LinkedList existingColumnNames = table.columnNames;
        LinkedList newColumnNames = new LinkedList();
        for (int i = 0; i < existingColumnNames.size(); i++) {
            newColumnNames.add(existingColumnNames.get(i));
        }
        Table finaltable = new Table(newColumnNames);
        String extra1Type;
        if (Character.toString(extra1.charAt(0)).equals("'")
                && Character.toString(extra1.charAt(extra1.length() - 1)).equals("'")) {
            extra1 = extra1.replaceAll("'", "");
            extra1Type = "string";
        } else {
            extra1Type = returnType(table, extra1);
            if (extra1Type.equals("")) {
                String typeGuy = itemType(extra1);
                if (typeGuy.equals("string")) {
                    throw new RuntimeException("ERROR: invalid conditional");
                }
                extra1Type = typeGuy;
            }
        }
        String potentialcol = extra1 + " " + extra1Type;
        String typeFirstCol = returnType(table, columnName);
        String fullcolname = columnName + " " + typeFirstCol;
        if (!typeFirstCol.equals(extra1Type)
                && (typeFirstCol.equals("string") || extra1Type.equals("string"))) {
            throw new Error("ERROR: Can't compare unmatching types");
        }
        if (!finaltable.columnNames.contains(potentialcol)
                && extra1Type.equals("int")) {
            int extra = Integer.parseInt(extra1);
            runLiteralonTypeColumnComparison(table, fullcolname, comparison,
                    extra, finaltable);
        } else if (!finaltable.columnNames.contains(potentialcol)
                && extra1Type.equals("float")) {
            float extra = Float.parseFloat(extra1);
            runLiteralonTypeColumnComparison(table, fullcolname, comparison,
                    extra, finaltable);
        } else if (!finaltable.columnNames.contains(potentialcol)
                && extra1Type.equals("string")) {
            compareLiteralStringonString(table, finaltable, fullcolname,
                    comparison, extra1);
        } else if (finaltable.columnNames.contains(potentialcol)
                && extra1Type.equals("string")) {
            compareTwoColumnsStringOnString(table, finaltable, fullcolname,
                    comparison, potentialcol);
        } else if (finaltable.columnNames.contains(potentialcol)
                && !extra1Type.equals("string")) {
            compareTwoColumnsFloatOrInt(table, finaltable, fullcolname,
                    comparison, potentialcol);
        }

        return finaltable;
    }

    private String print(String tablename) {
        if (!tables.containsKey(tablename)) {
            return "ERROR: table doesn't exist";
        }
        Table table1 = tables.get(tablename);
        Iterator columnNamesIterator = table1.columnNames.iterator();
        String tableReturn = "";
        while (columnNamesIterator.hasNext()) {
            String colName = (String) columnNamesIterator.next();
            tableReturn += colName;
            if (columnNamesIterator.hasNext()) {
                tableReturn += ",";
            }
        }


        for (Row row : table1.rows.values()) {
            tableReturn += "\n";
            Iterator columnNameIterator = row.columns.keySet().iterator();
            while (columnNameIterator.hasNext()) {
                String colName = (String) columnNameIterator.next();
                if (colName.split("\\s+")[1].equals("string")) {
                    String checkValue = row.columns.get(colName);
                    if (checkValue.equals("NaN") || checkValue.equals("NOVALUE")) {
                        tableReturn += checkValue;
                    } else {
                        tableReturn = tableReturn + "'" + checkValue + "'";
                    }
                } else {
                    String checkValue = row.columns.get(colName);
                    if (checkValue.equals("NaN") || checkValue.equals("NOVALUE")) {
                        tableReturn += checkValue;
                    } else {
                        tableReturn += checkValue;
                    }
                }
                if (columnNameIterator.hasNext()) {
                    tableReturn += ",";
                }
            }
        }
        return tableReturn;
    }

    private String print(Table table1) {
        Iterator columnNamesIterator = table1.columnNames.iterator();
        String tableReturn = "";
        while (columnNamesIterator.hasNext()) {
            String colName = (String) columnNamesIterator.next();
            tableReturn += colName;
            if (columnNamesIterator.hasNext()) {
                tableReturn += ",";
            }
        }

        for (Row row : table1.rows.values()) {
            tableReturn += "\n";
            Iterator columnNameIterator = row.columns.keySet().iterator();
            while (columnNameIterator.hasNext()) {
                String colName = (String) columnNameIterator.next();
                if (colName.split("\\s+")[1].equals("string")) {
                    String checkValue = row.columns.get(colName);
                    if (checkValue.equals("NaN") || checkValue.equals("NOVALUE")) {
                        tableReturn += checkValue;
                    } else {
                        tableReturn = tableReturn + "'" + checkValue + "'";
                    }
                } else {
                    String checkValue = row.columns.get(colName);
                    if (checkValue.equals("NaN") || checkValue.equals("NOVALUE")) {
                        tableReturn += checkValue;
                    } else {
                        tableReturn += checkValue;
                    }
                }
                if (columnNameIterator.hasNext()) {
                    tableReturn += ",";
                }
            }
        }
        return tableReturn;
    }

    public class Table {

        LinkedHashMap<String, Row> rows;
        LinkedList<String> columnNames; /* the second string should be the TYPE of the column */
        private int size;

        public Table(LinkedList<String> names) {
            rows = new LinkedHashMap<String, Row>();
            columnNames = names;
            size = 0;
        }

        public void add(String key, Row value) {
            rows.put(key, value);
        }

        public int size() {
            return size;
        }

        public void increaseSize() {
            size++;
        }

        public void decreaseSize() {
            size--;
        }

    }

    public String transact(String query) {

        Matcher m;
        if ((m = CREATE_CMD.matcher(query)).matches()) {
            return createTable(m.group(1));
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
            try {
                return load(m.group(1) + ".tbl");
            } catch (IOException e) {
                return "ERROR: file not found";
            }
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
            return store(m.group(1));
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
            return drop(m.group(1));
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
            return insertInto(m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            return print(m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            return select(m.group(1));
        } else {
            return "ERROR: Malformed query";
        }
    }

    private static final String REST  = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND   = "\\s+and\\s+";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table "
            + REST),
            LOAD_CMD   = Pattern.compile("load "
                    + REST),
            STORE_CMD  = Pattern.compile("store "
                    + REST),
            DROP_CMD   = Pattern.compile("drop table "
                    + REST),
            INSERT_CMD = Pattern.compile("insert into "
                    + REST),
            PRINT_CMD  = Pattern.compile("print "
                    + REST),
            SELECT_CMD = Pattern.compile("select "
                    + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW  = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*"
            + "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS  = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+"
                    + "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+"
                    + "([\\w\\s+\\-*/'<>=!]+?(?:\\s+and\\s+"
                    + "[\\w\\s+\\-*/'<>=!]+?)*))?"),
            CREATE_SEL  = Pattern.compile("(\\S+)\\s+as select\\s+"
                    + SELECT_CLS.pattern()),
            INSERT_CLS  = Pattern.compile("(\\S+)\\s+values\\s+(.+?"
                    + "\\s*(?:,\\s*.+?\\s*)*)");

    private boolean isStringInt(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }


    private boolean isStringFloat(String s) {
        return s.contains("." + "");
    }


    private String returnType(Table t, String col) {
        LinkedList columnNames = t.columnNames;
        for (int i = 0; i < columnNames.size(); i++) {
            String currCol = (String) columnNames.get(i);
            if (currCol.split(" ")[0].equals(col)) {
                return currCol.split(" ")[1];
            }
        }
        return "";
    }


    private String potentialcolumnname(String potential) {
        String pcolumn;
        if (isStringFloat(potential)) {
            pcolumn = "float";
        } else if (isStringInt(potential)) {
            pcolumn = "int";
        } else {
            pcolumn = "string";
        }
        return pcolumn;
    }

    private void runLiteralonTypeColumnComparison(
            Table table, String columnName, String comparison, float num,
            Table finaltable) {
        int rownum = 0;
        for (int i = 0; i < table.rows.size(); i++) {
            if (comparison.equals("==")) {
                if (Float.parseFloat(table.rows.
                        get(Integer.toString(i)).columns.get(columnName)) == num) {
                    finaltable.rows.
                            put(Integer.toString(rownum),
                                    table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals("!=")) {
                if (Float.parseFloat(table.rows.
                        get(Integer.toString(i)).columns.get(columnName)) != num) {
                    finaltable.rows.
                            put(Integer.toString(rownum),
                                    table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals(">")) {
                if (Float.parseFloat(table.rows.
                        get(Integer.toString(i)).columns.get(columnName)) > num) {
                    finaltable.rows.
                            put(Integer.toString(rownum),
                                    table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals("<")) {
                if (Float.parseFloat(table.rows.
                        get(Integer.toString(i)).columns.get(columnName)) < num) {
                    finaltable.rows.
                            put(Integer.toString(rownum),
                                    table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals(">=")) {
                if (Float.parseFloat(table.rows.
                        get(Integer.toString(i)).columns.get(columnName)) >= num) {
                    finaltable.rows.
                            put(Integer.toString(rownum),
                                    table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else {
                if (Float.parseFloat(table.rows.
                        get(Integer.toString(i)).columns.get(columnName)) <= num) {
                    finaltable.rows.
                            put(Integer.toString(rownum),
                                    table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            }
        }
    }

    private void compareLiteralStringonString(
            Table table, Table finaltable, String columnName, String comparison, String str) {
        int rownum = 0;
        for (int i = 0; i < table.rows.size(); i++) {
            String val1 = table.rows.get(Integer.toString(i)).columns.get(columnName);
            int comp = val1.compareTo(str);
            if (comparison.equals("==")) {
                if (comp == 0) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals("!=")) {
                if (comp != 0) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals(">=")) {
                if (comp >= 0 || val1.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals("<=")) {
                if (comp <= 0 || str.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals(">")) {
                if (comp > 0 || val1.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else {
                if (comp < 0 || str.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            }
        }
    }

    private void compareTwoColumnsFloatOrInt(
            Table table, Table finaltable, String columnName, String comparison,
            String columnName2) {

        int rownum = 0;
        for (int i = 0; i < table.rows.size(); i++) {
            String val1 = table.rows.get(Integer.toString(i)).columns.get(columnName);
            String val2 = table.rows.get(Integer.toString(i)).columns.get(columnName2);
            Float float1 = Float.parseFloat(val1);
            Float float2 = Float.parseFloat(val2);
            if (comparison.equals("==")) {
                if (float1.equals(float2)) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals("!=")) {
                if (!float1.equals(float2)) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals(">")) {
                if (float1 > float2 || val1.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals("<")) {
                if (float1 < float2 || val2.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals(">=")) {
                if (float1 >= float2 || val1.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else {
                if (float1 <= float2 || val2.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            }
        }
    }
    private void compareTwoColumnsStringOnString(
            Table table, Table finaltable, String columnName, String comparison,
            String columnName2) {
        int rownum = 0;
        for (int i = 0; i < table.rows.size(); i++) {
            String val1 = table.rows.get(Integer.toString(i)).columns.get(columnName);
            String val2 = table.rows.get(Integer.toString(i)).columns.get(columnName2);
            if (comparison.equals("==")) {
                if (val1.compareTo(val2) == 0) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals("!=")) {
                if (val1.compareTo(val2) != 0) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals(">")) {
                if (val1.compareTo(val2) > 0 || val1.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals("<")) {
                if (val1.compareTo(val2) < 0 || val2.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else if (comparison.equals(">=")) {
                if (val1.compareTo(val2) >= 0 || val1.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            } else {
                if (val1.compareTo(val2) <= 0 || val2.equals("NaN")) {
                    finaltable.rows.put(Integer.toString(rownum),
                            table.rows.get(Integer.toString(i)));
                    rownum++;
                }
            }
        }
    }

    public static void main(String[] args) {
        Database db = new Database();
        db.transact("create new table t1 (firstname string, lastname string)");
        db.transact("insert into t1 values farbod, nowzad");
        db.transact("insert into t1 values efe, toros");
        db.transact("insert into t1 values brian,bakar");

        db.transact("create new table t2 (firstname ");
        db.transact("");
        db.transact("");
        db.transact("");
    }
}
