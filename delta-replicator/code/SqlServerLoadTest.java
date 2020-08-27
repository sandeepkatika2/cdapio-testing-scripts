import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SqlServerLoadTest {
  private static final String DB = "sqlserver_db";
  private static final String TABLE_PREFIX = "mytable";
  private static final int INIT_DELAY_IN_SECONDS = 0;
  private static final int PERIOD_IN_SECONDS = 1;
  private static List<AtomicLong> counterList;

  public static void main(String[] args) {
    String host = args[0];
    String user = args[1];
    String password = args[2];
    String timezone = args[3];
    int port = Integer.parseInt(args[4]);
    int numOfTables = Integer.parseInt(args[5]);
    int rowSizeInBytes = Integer.parseInt(args[6]);
    int terminateDurationInMins = Integer.parseInt(args[7]);
    int batchSize = Integer.parseInt(args[8]);
    double updateFactor = Double.parseDouble(args[9]);
    int chunkSize = 50000;
    int remain = rowSizeInBytes % chunkSize;
    int numOfBodyColumns = remain == 0 ? rowSizeInBytes / chunkSize : rowSizeInBytes / chunkSize + 1;
    Random random = new Random();

    String createStatement = "CREATE TABLE %s (id int PRIMARY KEY IDENTITY(1,1), ";
    String queryStatement = "INSERT INTO %s (";
    for (int i = 0; i < numOfBodyColumns; i++) {
      createStatement += "body" + (i + 1) + " text, ";
      queryStatement += "body" + (i + 1) + ", ";
    }
    createStatement += "create_at date null)";
    queryStatement += "create_at) VALUES (";
    for (int i = 0; i < numOfBodyColumns; i++) {
      queryStatement += "?, ";
    }
    queryStatement += "?)";
    final String finalQueryStatement = queryStatement;

    try {
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
      String connURL = String.format("jdbc:sqlserver://%s:%d", host, port);;
      Properties properties = new Properties();
      properties.put("user", user);
      properties.put("password", password);
      properties.put("serverTimezone", timezone);

      // create database
      try (Connection connection = DriverManager.getConnection(connURL, properties);
           Statement statement = connection.createStatement()) {
        statement.execute("IF (DB_ID('" + DB + "') IS NOT NULL)\n" +
                            "\tBEGIN\n" +
                            "\tUSE master;\n" +
                            "\tALTER DATABASE " + DB + "\n" +
                            "\tSET SINGLE_USER \n" +
                            "\tWITH ROLLBACK IMMEDIATE\n" +
                            "\tDROP DATABASE " + DB + ";\n" +
                            "\tEND");
        statement.execute("CREATE DATABASE " + DB);
        System.out.println(String.format("CREATED DATABASE:%s", DB));
      }

      String finalConnURL = connURL + ";databaseName=" + DB;
      // create tables
      try (Connection connection = DriverManager.getConnection(finalConnURL, properties)) {
        for (int i = 1; i <= numOfTables; i++) {
          try (Statement statement = connection.createStatement()) {
            statement.execute(
              String.format(createStatement, TABLE_PREFIX + i));
          }
        }
        System.out.println(String.format("CREATED %d TABLE(S)", numOfTables));

        try (Statement statement = connection.createStatement()) {
          statement.execute("EXEC sys.sp_cdc_enable_db");
          System.out.println(String.format("ENABLED CDC FOR DATABASE:%s", DB));
        }

        for (int i = 1; i <= numOfTables; i++) {
          try (Statement statement = connection.createStatement()) {
            statement.execute(
              String.format("EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'%s', @role_name = NULL", TABLE_PREFIX + i));
          }
        }
        System.out.println("ENABLED CDC FOR ALL TABLE(S)");

        for (int i = 1; i <= numOfTables; i++) {
          String insert = String.format(queryStatement, TABLE_PREFIX + i);
          try (PreparedStatement ps = connection.prepareStatement(insert)) {
            int index = 1;
            for (int j = 1; j <= numOfBodyColumns; j++) {
              if (j == numOfBodyColumns && remain > 0) {
                continue;
              }
              String sameBody = getAlphaNumericString(chunkSize);
              ps.setString(index++, sameBody);
            }
            if (remain != 0) {
              ps.setString(index++, getAlphaNumericString(remain));
            }
            ps.setDate(index, Date.valueOf(LocalDate.now()));
            ps.execute();
          }
        }
        System.out.println("FINISHED PRELOAD ONE RECORD TO EACH TABLE");
      }

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

      ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      final ThreadFactory threadFactory = runnable -> {
        final Thread thread = new Thread(runnable, "load-test-daemon");
        thread.setDaemon(true);
        return thread;
      };
      // create a fixed thread pool, size is equal to num of tables
      ExecutorService executorService = Executors.newFixedThreadPool(numOfTables, threadFactory);
      counterList = new ArrayList<>(numOfTables);
      for (int i = 0; i < numOfTables; i++) {
        counterList.add(new AtomicLong());
      }

      // insert sample data
      // String insert = String.format("INSERT INTO %s VALUES (?, ?, ?)", TABLE);
      // update sample data
      // String update = String.format("UPDATE %s SET name = 'product' WHERE id = ?", TABLE);
      // delete sample data
      // String delete = String.format("DELETE FROM %s WHERE id = ?", TABLE;
      Runnable task = () -> {
        List<Future> futures = new ArrayList<>(numOfTables);
        for (int i = 1; i <= numOfTables; i++) {
          final int tableId = i;
          futures.add(executorService.submit((Callable<Void>) () -> {
            try (Connection connection = DriverManager.getConnection(finalConnURL, properties)) {
              long startTime = System.currentTimeMillis();
              String insert = String.format(finalQueryStatement, TABLE_PREFIX + tableId);
              try (PreparedStatement ps = connection.prepareStatement(insert)) {
                for (int j = 1; j <= batchSize; j++) {
                  int index = 1;
                  for (int k = 1; k <= numOfBodyColumns; k++) {
                    if (k == numOfBodyColumns && remain > 0) {
                      continue;
                    }
                    String sameBody = getAlphaNumericString(chunkSize);
                    ps.setString(index++, sameBody);
                  }
                  if (remain != 0) {
                    ps.setString(index++, getAlphaNumericString(remain));
                  }
                  ps.setDate(index, Date.valueOf(LocalDate.now()));
                  ps.addBatch();
                }
                ps.executeBatch();
              }
              long count = counterList.get(tableId - 1).incrementAndGet();
              long endTime = System.currentTimeMillis();
              System.out.println(String.format("Inserted %d row(s) with size %d bytes to table:%s using %fs", count * batchSize,
                                               rowSizeInBytes, TABLE_PREFIX + tableId, (endTime - startTime) / 1000.0));
              startTime = System.currentTimeMillis();
              String update = String.format("UPDATE %s SET body1 = 'xyz123' WHERE id = ?", TABLE_PREFIX + tableId);
              int updateBatchSize = Math.max(1, (int)(updateFactor * batchSize));
              try (PreparedStatement ps = connection.prepareStatement(update)) {
                for (int j = 1; j <= updateBatchSize; j++) {
                  int randId = random.nextInt((int)count * batchSize);
                  ps.setInt(1, randId);
                  ps.addBatch();
                }
                ps.executeBatch();
              }
              endTime = System.currentTimeMillis();
              System.out.println(String.format("Updated %d row(s) with 'body1' field to table:%s using %fs",
                                               count * updateBatchSize, TABLE_PREFIX + tableId, (endTime - startTime) / 1000.0));
            } catch (Exception e) {
              e.printStackTrace();
            }
            return null;
          }));
        }

        Exception exception = null;
        for (Future future : futures) {
          try {
            future.get();
          } catch (Exception e) {
            if (exception != null) {
              exception.addSuppressed(e);
            } else {
              exception = e;
            }
          }
        }

        if (exception != null) {
          exception.printStackTrace();
        }
      };

      ScheduledFuture<?> future =
        scheduledExecutorService.scheduleAtFixedRate(task, INIT_DELAY_IN_SECONDS, PERIOD_IN_SECONDS, TimeUnit.SECONDS);

      Thread.sleep(TimeUnit.MINUTES.toMillis(terminateDurationInMins));
      System.out.println("Time up, shutdown executors!");
      future.cancel(true);
      scheduledExecutorService.shutdown();
      executorService.shutdown();
      try {
        scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
        executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
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
