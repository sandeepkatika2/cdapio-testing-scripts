import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class TableCreator {
  private static final String DB = "mydb_test";
  private static final String TABLE_PREFIX = "mytable";

  public static void main(String[] args) {
    String host = args[0];
    String user = args[1];
    String password = args[2];
    String timezone = args[3];
    int port = Integer.parseInt(args[4]);
    int numOfTables = Integer.parseInt(args[5]);

    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
      String connURL = String.format("jdbc:mysql://%s:%d", host, port);
      Properties properties = new Properties();
      properties.put("user", user);
      properties.put("password", password);
      properties.put("serverTimezone", timezone);

      // create database
      try (Connection connection = DriverManager.getConnection(connURL, properties);
           Statement statement = connection.createStatement()) {
        statement.execute("DROP DATABASE IF EXISTS " + DB);
        statement.execute("CREATE DATABASE " + DB);
        System.out.println(String.format("CREATED DATABASE:%s", DB));
      }

      String finalConnURL = connURL + "/" + DB;
      // create tables
      try (Connection connection = DriverManager.getConnection(finalConnURL, properties)) {
        for (int i = 1; i <= numOfTables; i++) {
          try (Statement statement = connection.createStatement()) {
            statement.execute(
              String.format("CREATE TABLE %s (id int PRIMARY KEY AUTO_INCREMENT, body text, create_at date null)",
                            TABLE_PREFIX + i));
          }
        }
        System.out.println(String.format("CREATED %d TABLE(S)", numOfTables));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
