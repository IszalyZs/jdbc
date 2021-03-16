package com.mycompany.jdbc;

public class Main {
    public static void main(String[] args) {
        JdbcManager jdbcManager = new JdbcManager();
        jdbcManager.showPeopleWithData();
        jdbcManager.showPeopleUnder18();
        jdbcManager.showPeopleFromNewYorkCity();
        jdbcManager.showPeopleWhoseNamesStartWithC();
        jdbcManager.showPeopleOrderedByName();
    }
}
