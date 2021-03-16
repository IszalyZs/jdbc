package com.mycompany.jdbc;

import org.h2.command.dml.Update;

import java.sql.*;

public class JdbcManager {
    private final String DB_URL = "jdbc:h2:~/src/main/resources/";
    private final String USER = "user";
    private final String PASS = "password";
    private final String DB_NAME = "database";
    private final String TABLE_NAME = "people";

    public JdbcManager() {
        initialize();
    }

    private void initialize() {
        String DROP = "DROP TABLE " + TABLE_NAME;
        String SQL = "CREATE TABLE " + TABLE_NAME + "(id SERIAL PRIMARY KEY, name VARCHAR, age SMALLINT, city VARCHAR) ";
        String INSERT = "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Bob', 20, 'Los Angeles');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Charlie', 18, 'New York City');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Alex', 25, 'Chicago');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Caroline', 17, 'New York City');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Alfred', 15, 'Houston');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Charlotte', 21, 'Los Angeles');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Susan', 40, 'New York City');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Helen', 35, 'Philadelphia');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Pedro', 13, 'San Diego');" +
                "INSERT INTO " + TABLE_NAME + " (name, age, city) VALUES ('Harry', 15, 'New York City');";

        try (Connection connection = DriverManager.getConnection(DB_URL + DB_NAME, USER, PASS);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(DROP);
            statement.execute(SQL);
            statement.executeUpdate(INSERT);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private ResultSet getResultSet(String sql) throws SQLException {
        return DriverManager.getConnection(DB_URL + DB_NAME, USER, PASS).createStatement().executeQuery(sql);
    }

    private void printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCont = resultSetMetaData.getColumnCount();
        while (resultSet.next()) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 1; i <= columnCont; i++) {
                stringBuilder.append(resultSet.getString(resultSetMetaData.getColumnName(i))).append("\t");
            }
            System.out.println(stringBuilder);
        }
        System.out.println();
    }

    private void runResultSet(String SQL) {
        try {
            ResultSet resultSet = getResultSet(SQL);
            printResultSet(resultSet);
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void showPeopleWithData() {
        runResultSet("select * from " + TABLE_NAME);
    }

    public void showPeopleUnder18() {
        runResultSet("select name,age from " + TABLE_NAME + " where age<18");
    }

    public void showPeopleFromNewYorkCity() {
        runResultSet("select name,city from " + TABLE_NAME + " where city='New York City'");
    }

    public void showPeopleWhoseNamesStartWithC() {
        runResultSet("select name from " + TABLE_NAME + " where name like 'C%'");
    }

    public void showPeopleOrderedByName() {
        runResultSet("select name from " + TABLE_NAME + " order by name");
    }
}
