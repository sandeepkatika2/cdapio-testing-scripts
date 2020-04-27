import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This program is used to automate the process of creating database/tables and inserting data/event periodically.
 * Here are some variables that can be changed:
 * 1. Host: ip address
 * 2. USER : MySQL database user
 * 3. PASSWORD : MySQL database password
 * 4. PORT: MySQL port number (default is 3306)
 * 5. NUM_OF_TABLES : # of tables to create under the database
 * 6. ROW_SIZE_IN_BYTES : size of each insert row in bytes
 * 7. PERIOD_IN_SECONDS: frequency of inserting a row to a table
 * 8. TERMINATE_IN_MINUTES: time period to terminate the program in minutes
 */
public class LoadGenerator {
  private static final String DB = "mydb_test";
  private static final String TABLE_PREFIX = "mytable";
  private static final int INIT_DELAY_IN_SECONDS = 0;
  private static final int PERIOD_IN_SECONDS = 1;
  private static AtomicLong counter = new AtomicLong();

  public static void main(String[] args) {
    String host = args[0];
    String user = args[1];
    String password = args[2];
    String timezone = args[3];
    int port = Integer.parseInt(args[4]);
    int numOfTables = Integer.parseInt(args[5]);
    int rowSizeInBytes = Integer.parseInt(args[6]);
    int terminateDurationInMins = Integer.parseInt(args[7]);

    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
      String connURL = String.format("jdbc:mysql://%s:%d/%s", host, port, DB);
      Properties properties = new Properties();
      properties.put("user", user);
      properties.put("password", password);
      properties.put("serverTimezone", timezone);

      try (Scanner scanner = new Scanner(System.in)) {
        while (true) {
          System.out.print("Do you want to start inserting rows into tables now, type yes/no:");
          String option = scanner.next();
          if ("no".equals(option) || "NO".equals(option)) {
            System.out.println("Exit!");
            return;
          } else if ("yes".equals(option) || "YES".equals(option)) {
            System.out.println("Start inserting rows into tables!");
            break;
          } else {
            System.out.println("Do not recognize what you have typed, please try it again!");
          }
        }
      }

      ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
      // insert sample data
      // String insert = String.format("INSERT INTO %s VALUES (?, ?, ?)", TABLE);
      // update sample data
      // String update = String.format("UPDATE %s SET name = 'product' WHERE id = ?", TABLE);
      // delete sample data
      // String delete = String.format("DELETE FROM %s WHERE id = ?", TABLE;
      Runnable task = () -> {
        try (Connection connection = DriverManager.getConnection(connURL, properties)) {
          for (int i = 1; i <= numOfTables; i++) {
            String insert = String.format("INSERT INTO %s (body, create_at) VALUES (?, ?)", TABLE_PREFIX + i);
            try (PreparedStatement ps = connection.prepareStatement(insert)) {
              ps.setString(1, getAlphaNumericString(rowSizeInBytes - 7));
              ps.setDate(2, Date.valueOf(LocalDate.now()));
              ps.execute();
            }
          }
          counter.incrementAndGet();
          System.out.println(String.format("Inserted %d row(s) with size %d bytes to each table", counter.get(),
                                           rowSizeInBytes));
        } catch (Exception e) {
          e.printStackTrace();
        }
      };

      ScheduledFuture<?> future =
        executor.scheduleAtFixedRate(task, INIT_DELAY_IN_SECONDS, PERIOD_IN_SECONDS, TimeUnit.SECONDS);

      while (true) {
        Thread.sleep(1000);
        if (counter.get() ==  terminateDurationInMins * 60 / PERIOD_IN_SECONDS) {
          System.out.println("Task done!");
          future.cancel(true);
          executor.shutdown();
          break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // function to generate a random string of length n
  private static String getAlphaNumericString(int n) {
    // chose a Character random from this String
    String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      + "0123456789"
      + "abcdefghijklmnopqrstuvxyz";

    // create StringBuffer size of AlphaNumericString
    StringBuilder sb = new StringBuilder(n);

    for (int i = 0; i < n; i++) {
      // generate a random number between
      // 0 to AlphaNumericString variable length
      int index = (int)(AlphaNumericString.length() * Math.random());

      // add Character one by one in end of sb
      sb.append(AlphaNumericString.charAt(index));
    }

    return sb.toString();
  }
}
