package db61b;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.LinkedHashSet;


import static db61b.Utils.*;

/** A single table in a database.
 *  @author Thomas Wu
 */
class Table implements Iterable<Row> {



    /** A new Table whose columns are given by COLUMNTITLES, which may
     *  not contain dupliace names. */
    Table(String[] columnTitles) {
        for (int i = columnTitles.length - 1; i >= 1; i -= 1) {
            for (int j = i - 1; j >= 0; j -= 1) {
                if (columnTitles[i].equals(columnTitles[j])) {
                    throw error("duplicate column name: %s",
                                columnTitles[i]);
                }
            }
        }
        _columnTitles = columnTitles;
        _table = new ArrayList<String[]>();
        _nextIndex = 0;
    }

    /** A new Table whose columns are give by COLUMNTITLES. */
    Table(List<String> columnTitles) {
        this(columnTitles.toArray(new String[columnTitles.size()]));
    }

    /** Return the number of columns in this table. */
    public int columns() {
        return _columnTitles.length;
    }

    /** Return the title of the Kth column.  Requires 0 <= K < columns(). */
    public String getTitle(int k) {
        return _columnTitles[k];
    }

    /** Returns the string within the array from the J th row and
      * K th element of the table. */
    public String getArray(int k, int j) {
        return _table.get(j)[k];
    }

    /** Return the number of the column whose title is TITLE, or -1 if
     *  there isn't one. */
    public int findColumn(String title) {
        for (int i = 0; i < _columnTitles.length; i += 1) {
            if (_columnTitles[i].equals(title)) {
                return i;
            }
        }
        return -1;
    }

    /** Return the number of Rows in this table. */
    public int size() {
        return _nextIndex;
    }

    /** Returns an iterator that returns my rows in an unspecfied order. */
    @Override
    public Iterator<Row> iterator() {
        return _rows.iterator();
    }

    /** Add ROW to THIS if no equal row already exists.  Return true if anything
     *  was added, false otherwise. */
    public boolean add(Row row) {
        if (row.size() != this.columns()) {
            throw error("inserted row has wrong length");
        }
        for (int i = 0; i < _nextIndex; i += 1) {
            if (row.equals(gR(i))) {
                return false;
            }
        }
        _table.add(row.toArray());
        _nextIndex += 1;
        return true;
    }

    /** Read the contents of the file NAME.db, and return as a Table.
     *  Format errors in the .db file cause a DBException. */
    static Table readTable(String name) {
        BufferedReader input;
        Table table;
        input = null;
        table = null;
        try {
            input = new BufferedReader(new FileReader(name + ".db"));
            String header = input.readLine();
            if (header == null) {
                throw error("missing header in DB file");
            }
            String[] columnNames = header.split(",");
            table = new Table(columnNames);
            String line = null;
            while ((line = input.readLine()) != null) {
                String[] rowArray = line.split(",");
                table.add(new Row(rowArray));
            }
        } catch (FileNotFoundException e) {
            throw error("could not find %s.db", name);
        } catch (IOException e) {
            throw error("problem reading from %s.db", name);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    /* Ignore IOException */
                }
            }
        }
        return table;
    }

    /** Write the contents of TABLE into the file NAME.db. Any I/O errors
     *  cause a DBException. */
    void writeTable(String name) {
        PrintStream output;
        output = null;
        try {
            String sep;
            sep = "";
            output = new PrintStream(name + ".db");
            for (int a = 0; a < _columnTitles.length - 1; a++) {
                output.print(_columnTitles[a] + ",");
            }
            output.print(_columnTitles[_columnTitles.length - 1]);
            output.println();
            for (int i = 0; i < this.size(); i++) {
                String[] line = getK(i);
                for (int j = 0; j < line.length - 1; j++) {
                    output.append(line[j]);
                    output.append(",");
                }
                output.append(line[line.length - 1]);
                output.println();
            }
        } catch (IOException e) {
            throw error("trouble writing to %s.db", name);
        } finally {
            if (output != null) {
                output.close();
            }
        }
    }

    /** Print my contents on the standard output. */
    void print() {
        int colunmns = this.columns();

        for (int a = 0; a < _table.size(); a += 1) {
            for (int i = 0; i < colunmns; i += 1) {
                System.out.print(" " + _table.get(a)[i]);
            }
            System.out.println();
        }
    }

    /** Return a new Table whose columns are COLUMNNAMES, selected from
     *  rows of this table that satisfy CONDITIONS. */
    Table select(List<String> columnNames, List<Condition> conditions) {
        Table result = new Table(columnNames);
        List<Column> lstOfCols = new ArrayList<Column>();
        for (int i = 0; i < columnNames.size(); i++) {
            lstOfCols.add(new Column(columnNames.get(i), this));
        }
        for (int j = 0; j < this.size(); j++) {
            Row temp = this.gR(j);
            boolean pass = true;
            for (int p = 0; p < conditions.size(); p++) {
                if (conditions.get(p).test(conditions, temp)) {
                    pass &= true;
                }
                if (!conditions.get(p).test(conditions, temp)) {
                    pass &= false;
                }
            }
            if (conditions != null) {
                if (!Condition.test(conditions, temp)) {
                    pass = false;
                }
            }
            if (pass) {
                Row temp2 = new Row(lstOfCols, temp);
                if (result.size() == 0) {
                    result.add(temp2);
                } else {
                    boolean flag = false;
                    for (int k = 0; k < result.size(); k++) {
                        if (Arrays.deepToString(temp2.toArray()).equals(
                                Arrays.deepToString(result.getK(k)))) {
                            flag = true;
                        }
                    }
                    if (!flag) {
                        result.add(temp2);
                    }
                }
            }
        }
        return result;
    }

    /** Get the K th string array from tables. It corresponds to the data
      * the row. GETK returns this string array. */
    String[] getK(int k) {
        return _table.get(k);
    }

    /** Actually gets the row object, from the I th row of the table, and
      * GR returns this object. */
    Row gR(int i) {
        return new Row(_table.get(i));
    }

    /** Return a new Table whose columns are COLUMNNAMES, selected
     *  from pairs of rows from this table and from TABLE2 that match
     *  on all columns with identical names and satisfy CONDITIONS. */
    Table select(Table table2, List<String> columnNames,
                 List<Condition> conditions) {
        Table result = new Table(columnNames);
        LinkedHashSet<String> giantNames = new LinkedHashSet<>();
        for (int a = 0; a < this.columns(); a++) {
            giantNames.add(this.getTitle(a));
        }
        for (int b = 0; b < table2.columns(); b++) {
            giantNames.add(table2.getTitle(b));
        }
        List<String> listNames = new ArrayList<String>(giantNames);
        Table giantTable = new Table(listNames);
        List<Column> column1 = new ArrayList<Column>();
        List<Column> column2 = new ArrayList<Column>();
        for (int i = 0; i < columnNames.size(); i++) {
            column1.add(new Column(columnNames.get(i), this, table2));
            column2.add(new Column(columnNames.get(i), table2, this));
        }
        for (int x = 0; x < this.size(); x++) {
            for (int y = 0; y < table2.size(); y++) {
                if (equijoin(column1, column2, this.gR(x), table2.gR(y))
                    && Condition.test(conditions, this.gR(x), table2.gR(y))) {
                    String[] aa = this.getK(x);
                    String[] bb = table2.getK(y);
                    List<String> aaa = new ArrayList<String>(Arrays.asList(aa));
                    List<String> bbb = new ArrayList<String>(Arrays.asList(bb));
                    boolean flag2 = false;
                    for (int a4 = 0; a4 < aa.length; a4++) {
                        for (int bbbb = 0; bbbb < bb.length; bbbb++) {
                            if (aaa.get(a4).equals(bbb.get(bbbb))) {
                                flag2 = true;
                            }
                        }
                    }
                    boolean flag3 = testNoSimilarColumns(this, table2);
                    if (flag2 || ((conditions.size() == 0)) & !flag3) {
                        String[] bothValues = cat(this.getK(x), table2.getK(y));
                        List<String> arrayBothVals = Arrays.asList(bothValues);
                        ArrayList<String> unqVal = new ArrayList<String>();
                        for (String rowData : arrayBothVals) {
                            if (!unqVal.contains(rowData)) {
                                unqVal.add(rowData);
                            }
                        }
                        String[] uniqueBV = new String[unqVal.size()];
                        uniqueBV = unqVal.toArray(uniqueBV);
                        giantTable.add(new Row(uniqueBV));
                    }
                }
            }
        }
        List<Column> lstOfCols = new ArrayList<Column>();
        for (int f = 0; f < columnNames.size(); f++) {
            lstOfCols.add(new Column(columnNames.get(f), giantTable));
        }
        for (int j = 0; j < giantTable.size(); j++) {
            Row temp3 = new Row(lstOfCols, giantTable.gR(j));
            if (result.size() == 0) {
                result.add(temp3);
            } else {
                boolean flag = false;
                for (int k = 0; k < result.size(); k++) {
                    if (Arrays.deepToString(temp3.toArray()).equals(
                        Arrays.deepToString(result.getK(k)))) {
                        flag = true;
                    }
                }
                if (!flag) {
                    result.add(temp3);
                }
            }
        }
        return result;
    }

    /** Helper function TESTNOSIMILARCOLUMNS checks to see if there are
      * no similar column names between two tables TBL and TBL2. It
      * returns FLAG is true if there isn't.
      */
    boolean testNoSimilarColumns(Table tbl, Table tbl2) {
        boolean flag = false;
        String[] thisName = tbl.getColumnNames();
        String[] tbl2Name = tbl2.getColumnNames();
        for (int i = 0; i < thisName.length; i++) {
            for (int j = 0; j < tbl2Name.length; j++) {
                if (thisName[i].equals(tbl2Name[j])) {
                    flag = true;;
                }
            }
        }
        return flag;
    }

    /** GETCOLUMNNAMES returns the names of the columns of the table. */
    String[] getColumnNames() {
        return _columnTitles;
    }

    /** Return true if the columns COMMON1 from ROW1 and COMMON2 from
     *  ROW2 all have identical values.  Assumes that COMMON1 and
     *  COMMON2 have the same number of elements and the same names,
     *  that the columns in COMMON1 apply to this table, those in
     *  COMMON2 to another, and that ROW1 and ROW2 come, respectively,
     *  from those tables. */
    private static boolean equijoin(List<Column> common1, List<Column> common2,
                                    Row row1, Row row2) {
        for (int i = 0; i < common1.size(); i++) {
            if (!common1.get(i).getFrom(row1, row2).equals(
                common2.get(i).getFrom(row2, row1))) {
                return false;
            }
        }
        return true;
    }


    /** CAT adds any amount of string ARRAYS, and returns RESULT as a new
      * string array. */
    private static String[] cat(String[]... arrays) {
        int lengh = 0;
        for (String[] array : arrays) {
            lengh += array.length;
        }
        String[] result = new String[lengh];
        int pos = 0;
        for (String[] array : arrays) {
            for (String element : array) {
                result[pos] = element;
                pos++;
            }
        }
        return result;
    }


    /** My rows. */
    private HashSet<Row> _rows = new HashSet<>();

    /** Decide to implement our _TABLE as an arraylist of strings. */
    private List<String[]> _table;

    /** Decide to implement the titles separately, as a string array. */
    private String[] _columnTitles;

    /** This is the counter for number of rows, used for many fcns. */
    private int _nextIndex;

}

