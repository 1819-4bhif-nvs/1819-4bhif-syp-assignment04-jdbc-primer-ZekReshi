package at.htl.schwarcz_jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HomeAutomationTest {

    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    static final String USER = "app";
    static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc() {
        try{
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Verbindung zur Datenbank nicht möglich:\n"
                    + e.getMessage() + "\n");
            System.exit(1);
        }

        try {
            Statement stmt = conn.createStatement();

            String sql = "CREATE TABLE room (" +
                    " id INT CONSTRAINT room_pk PRIMARY KEY," +
                    " name VARCHAR(255) NOT NULL)";

            stmt.execute(sql);

            sql = "CREATE TABLE sensortype (" +
                    " id INT CONSTRAINT sensortype_pk PRIMARY KEY," +
                    " type VARCHAR(255) NOT NULL)";

            stmt.execute(sql);

            sql = "CREATE TABLE sensor (" +
                    " id INT CONSTRAINT sensor_pk PRIMARY KEY," +
                    " room_id INT REFERENCES room ON DELETE SET NULL," +
                    " sensortype_id INT REFERENCES sensortype ON DELETE SET NULL)";

            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void teardownJdbc() {

        try {
            conn.createStatement().execute("DROP TABLE sensor");
            System.out.println("Tabelle SENSOR gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle SENSOR konnte nicht gelöscht werden:\n"
                    + e.getMessage() + "\n");
        }
        try {
            conn.createStatement().execute("DROP TABLE room");
            System.out.println("Tabelle ROOM gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle ROOM konnte nicht gelöscht werden:\n"
                    + e.getMessage() + "\n");
        }
        try {
            conn.createStatement().execute("DROP TABLE sensortype");
            System.out.println("Tabelle SENSORTYPE gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle SENSORTYPE konnte nicht gelöscht werden:\n"
                    + e.getMessage() + "\n");
        }
        try {
            if(conn != null || !conn.isClosed()) {
                conn.close();
                System.out.println("Goodbye!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDdl() {
        try {
            // Sensor-Metadata abfragen
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM sensor");
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            assertThat(rsMeta.getColumnCount(), is(3));
            assertThat(rsMeta.getColumnName(1), is("ID"));
            assertThat(rsMeta.getColumnTypeName(1), is("INTEGER"));
            assertThat(rsMeta.getColumnName(2), is("ROOM_ID"));
            assertThat(rsMeta.getColumnTypeName(2), is("INTEGER"));
            assertThat(rsMeta.getColumnName(3), is("SENSORTYPE_ID"));
            assertThat(rsMeta.getColumnTypeName(3), is("INTEGER"));

            // SensorType-Metadata abfragen
            stmt = conn.prepareStatement("SELECT * FROM sensortype");
            rs = stmt.executeQuery();
            rsMeta = rs.getMetaData();
            assertThat(rsMeta.getColumnCount(), is(2));
            assertThat(rsMeta.getColumnName(1), is("ID"));
            assertThat(rsMeta.getColumnTypeName(1), is("INTEGER"));
            assertThat(rsMeta.getColumnName(2), is("TYPE"));
            assertThat(rsMeta.getColumnTypeName(2), is("VARCHAR"));

            // Room-Metadata abfragen
            stmt = conn.prepareStatement("SELECT * FROM room");
            rs = stmt.executeQuery();
            rsMeta = rs.getMetaData();
            assertThat(rsMeta.getColumnCount(), is(2));
            assertThat(rsMeta.getColumnName(1), is("ID"));
            assertThat(rsMeta.getColumnTypeName(1), is("INTEGER"));
            assertThat(rsMeta.getColumnName(2), is("NAME"));
            assertThat(rsMeta.getColumnTypeName(2), is("VARCHAR"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDml() {
        try {
            // Rooms einfügen
            int countInserts = 0;

            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO room (id, name) VALUES (1,'Badezimmer')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO room (id, name) VALUES (2,'Schlafzimmer')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO room (id, name) VALUES (3,'Wohnzimmer')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO room (id, name) VALUES (4,'Vorzimmer')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO room (id, name) VALUES (5,'Kinderzimmer')";
            countInserts += stmt.executeUpdate(sql);

            assertThat(countInserts, is(5));

            // SensorTypes einfügen
            countInserts = 0;

            stmt = conn.createStatement();
            sql = "INSERT INTO sensortype (id, type) VALUES (1,'Temperatur')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO sensortype (id, type) VALUES (2,'Stickstoff')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO sensortype (id, type) VALUES (3,'Licht')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO sensortype (id, type) VALUES (4,'Feuchtigkeit')";
            countInserts += stmt.executeUpdate(sql);

            assertThat(countInserts, is(4));

            // Sensors einfügen
            countInserts = 0;

            stmt = conn.createStatement();
            sql = "INSERT INTO sensor (id, room_id, sensortype_id) VALUES (1,2,1)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO sensor (id, room_id, sensortype_id) VALUES (2,2,2)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO sensor (id, room_id, sensortype_id) VALUES (3,1,3)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO sensor (id, room_id, sensortype_id) VALUES (4,4,1)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO sensor (id, room_id, sensortype_id) VALUES (5,3,4)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO sensor (id, room_id, sensortype_id) VALUES (6,5,1)";
            countInserts += stmt.executeUpdate(sql);

            assertThat(countInserts, is(6));

            // Rooms abfragen
            PreparedStatement pstmt = conn.prepareStatement("SELECT id, name FROM room");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("Badezimmer"));
            rs.next();
            assertThat(rs.getString("name"), is("Schlafzimmer"));
            rs.next();
            assertThat(rs.getString("name"), is("Wohnzimmer"));
            rs.next();
            assertThat(rs.getString("name"), is("Vorzimmer"));
            rs.next();
            assertThat(rs.getString("name"), is("Kinderzimmer"));

            // Sensortypes abfragen
            pstmt = conn.prepareStatement("SELECT id, type FROM sensortype");
            rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("type"), is("Temperatur"));
            rs.next();
            assertThat(rs.getString("type"), is("Stickstoff"));
            rs.next();
            assertThat(rs.getString("type"), is("Licht"));
            rs.next();
            assertThat(rs.getString("type"), is("Feuchtigkeit"));

            // Sensors abfragen
            pstmt = conn.prepareStatement("SELECT sensor.id, name, type FROM sensor" +
                            " JOIN room ON(room_id = room.id)" +
                            " JOIN sensortype ON(sensortype_id = sensortype.id)" +
                            " ORDER BY sensor.id");
            rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("Schlafzimmer"));
            assertThat(rs.getString("type"), is("Temperatur"));
            rs.next();
            assertThat(rs.getString("name"), is("Schlafzimmer"));
            assertThat(rs.getString("type"), is("Stickstoff"));
            rs.next();
            assertThat(rs.getString("name"), is("Badezimmer"));
            assertThat(rs.getString("type"), is("Licht"));
            rs.next();
            assertThat(rs.getString("name"), is("Vorzimmer"));
            assertThat(rs.getString("type"), is("Temperatur"));
            rs.next();
            assertThat(rs.getString("name"), is("Wohnzimmer"));
            assertThat(rs.getString("type"), is("Feuchtigkeit"));
            rs.next();
            assertThat(rs.getString("name"), is("Kinderzimmer"));
            assertThat(rs.getString("type"), is("Temperatur"));
       } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
