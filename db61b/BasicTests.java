package db61b;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;



/** Tests basic functionality.
 *  @author Thomas Wu
 */

public class BasicTests {
    /** Tests basic functionality including the row class.
     */

    @Test
    public void testRow() {
        Row r = new Row(new String[]{"Josh", "is", "writing", "this", "test."});
        assertEquals(5, r.size());
        assertEquals("is", r.get(1));
        assertEquals(true, r.equals(r));
        assertEquals(false, r.equals(new Row(new String[]{"Hello."})));
        assertEquals(true, r.equals(new Row(
            new String[]{"Josh", "is", "writing", "this", "test."})));

        Row s = new Row(new String[]{"Josh", "104", "hi"});
        assertEquals(false, r.equals(s));
        Row t = new Row(new String[]{"Josh", "is", "writing", "this", "test."});
        assertEquals(true, r.equals(t));

    }

    @Test
    public void testTable() {
        Table t = new Table(new String[]{"Name", "DOB", "SID", "CCN"});
        assertEquals(4, t.columns());
        assertEquals("DOB", t.getTitle(1));
        assertEquals(-1, t.findColumn("BLEH"));
        assertEquals(2, t.findColumn("SID"));
        assertEquals(0, t.size());

        Row tommy = new Row(
            new String[]{"TOMMY", "May 12", "24329578", "No clue."});
        t.add(tommy);
        assertEquals(1, t.size());
        t.add(tommy);
        assertEquals(1, t.size());

        Table sched = Table.readTable("schedule");
        assertEquals(8, sched.size());
        sched.print();
    }

    @Test
    public void testDatabase() {
        Database testingDatabase = new Database();
        assertEquals(null, testingDatabase.get("hello"));
        Table t = new Table(new String[]{"Name", "DOB", "SID", "CCN"});
        Row tommy = new Row(
            new String[]{"TOMMY", "May 12", "24329578", "No clue."});
        t.add(tommy);

        testingDatabase.put("Tommy", t);

        Table tActual = testingDatabase.get("Tommy");
        assertEquals(tActual, t);
        assertEquals(1, tActual.size());

        Table sched = Table.readTable("schedule");
        testingDatabase.put("schedule", sched);

        Table schedActual = testingDatabase.get("schedule");
        assertEquals(8, schedActual.size());
        assertEquals(sched, schedActual);
    }

    @Test
    public void testRowComplicated() {
        Database testingDatabase = new Database();
        Table t = new Table(new String[]{"Name", "DOB", "SID", "CCN"});
        Row tommy = new Row(
            new String[]{"TOMMY", "May 12", "24329578", "No clue."});
        t.add(tommy);
        testingDatabase.put("Tommy", t);


        Column cf = new Column("Name", t);
        Column cs = new Column("SID", t);

        ArrayList<Column> lstOfCol = new ArrayList<Column>();
        lstOfCol.add(cf);
        lstOfCol.add(cs);

        Row testRow = new Row(lstOfCol, tommy);
        assertEquals(2, testRow.size());
        assertEquals("TOMMY", testRow.get(0));
        assertEquals("24329578", testRow.get(1));

        Table tstSomething = new Table(new String[]{"Name", "SID"});
        tstSomething.add(testRow);
        tstSomething.print();


    }


    @Test
    public void testSelectNoCond() {
        Database testingDatabase = new Database();
        Table t = new Table(new String[]{"Name", "DOB", "SID", "CCN"});
        Row tommy = new Row(
            new String[]{"TOMMY", "May 12", "24329578", "No clue."});
        Row kevin = new Row(
            new String[]{"Kevin", "May 12", "222222", "No Cluetoo"});
        Row mellisa = new Row(
            new String[]{"MELLISA", "May 13", "108429", "CCN3"});
        Row mellisa2 = new Row(
            new String[]{"MELLISA", "May 13", "108429", "CCN3"});
        t.add(tommy);
        t.add(kevin);
        t.add(mellisa);
        t.add(mellisa2);

        testingDatabase.put("Tommy", t);

        List<String> strss = new ArrayList<String>();
        List<Condition> conds = new ArrayList<Condition>();
        strss.add("Name");
        Table newTbl = t.select(strss, conds);
        newTbl.print();

    }


    @Test
    public void testSelectTwoTables() {
        Database testingDatabase = new Database();
        Table t = new Table(new String[]{"Name", "DOB", "SID", "CCN"});
        Row tommy = new Row(
            new String[]{"Tommy", "May 12", "24329578", "No clue."});
        Row kevin = new Row(
            new String[]{"Kevin", "May 12", "222222", "No Cluetoo"});
        Row mellisa = new Row(
            new String[]{"MELLISA", "May 13", "108429", "CCN3"});
        Row mellisa2 = new Row(
            new String[]{"MELLISA", "May 13", "108429", "CCN3"});
        t.add(tommy);
        t.add(kevin);
        t.add(mellisa);
        t.add(mellisa2);

        testingDatabase.put("Identities", t);

        Table s = new Table(new String[] {"Name", "Class", "Year"});
        s.add(new Row(new String[]{"Tommy", "CS61B", "2014"}));
        s.add(new Row(new String[]{"Kevin", "CS61A", "2012"}));
        s.add(new Row(new String[]{"MELLISA", "CS61B", "2014"}));
        testingDatabase.put("", s);

        List<String> colnms = new ArrayList<String>();
        colnms.add("Name");
        colnms.add("Class");

        List<Condition> conds = new ArrayList<Condition>();
        conds.add(new Condition(new Column("Name", t, s), "=", "Tommy"));


        Table q = t.select(s, colnms, conds);
        assertEquals(1, q.size());
        assertEquals("Tommy", q.getArray(0, 0));

    }

    @Test
    public void testWriteTable() {
        Table s = new Table(new String[] {"Name", "Class", "Year"});
        s.add(new Row(new String[]{"Tommy", "CS61B", "2014"}));
        s.add(new Row(new String[]{"Kevin", "CS61A", "2012"}));
        s.add(new Row(new String[]{"MELLISA", "CS61B", "2014"}));

        s.writeTable("testingWriteTable");

        Table readtbl = Table.readTable("testingWriteTable");

        System.out.println("This is s");
        s.print();
        System.out.println();
        System.out.println("This is readtbl");
        readtbl.print();




    }





    public static void main(String[] args) {
        System.exit(ucb.junit.textui.runClasses(BasicTests.class));
    }
}
