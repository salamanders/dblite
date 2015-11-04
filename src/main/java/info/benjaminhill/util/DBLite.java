package info.benjaminhill.util;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.sqlite.SQLiteConnection;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

/**
 *
 * <code>
DBLite.DB.update("drop table if exists person");
DBLite.DB.update("create table person (id integer PRIMARY KEY, name string UNIQUE)");
DBLite.DB.update("insert into person values(?, ?)", 1, "leo");
DBLite.DB.update("insert into person values(?, ?)", 2, "yui");
System.out.println(DBLite.DB.select("SELECT * FROM person WHERE id>?", 0));
</code>
 *
 * @author Benjamin Hill
 */
public enum DBLite implements AutoCloseable {
  DB;

  private static final String FILE_NAME = "mydb.sqlite";
  public final Connection conn;

  private DBLite() {
    try {
      conn = DriverManager.getConnection("jdbc:sqlite:" + FILE_NAME);
      ((SQLiteConnection) conn).setBusyTimeout(35 * 1_000);

      try (final Statement stmt = conn.createStatement()) {
        stmt.execute("PRAGMA synchronous = OFF;"); // speed
      }
      try (final Statement stmt = conn.createStatement()) {
        stmt.execute("PRAGMA journal_mode = OFF;"); // speed
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    conn.close();
  }

  public boolean exists() {
    return new File(FILE_NAME).canRead();
  }

  public long selectLong(final String sql, final Object... args) {
    Preconditions.checkNotNull(sql);
    Preconditions.checkArgument(sql.toLowerCase().contains("select"));
    try (final PreparedStatement pstmt = conn.prepareStatement(sql);) {
      for (int i = 0; i < args.length; i++) {
        pstmt.setObject(i + 1, args[i]);
      }
      pstmt.execute();
      // First result is a ResultSet
      try (final ResultSet rs = pstmt.getResultSet()) {
        rs.next();
        return rs.getLong(1);
      }
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Select an arbitrary table.  First column must be row identifier.
   */
  public Table<String, String, Object> selectTable(final String sql, final Object... args) {
    Preconditions.checkNotNull(sql);
    Preconditions.checkArgument(sql.toLowerCase().contains("select"));
    try {
      final Table<String, String, Object> result = HashBasedTable.create();
      try (final PreparedStatement pstmt = conn.prepareStatement(sql);) {
        for (int i = 0; i < args.length; i++) {
          pstmt.setObject(i + 1, args[i]);
        }
        try (final ResultSet rs = pstmt.executeQuery()) {
          final ResultSetMetaData rsmd = rs.getMetaData();
          
          final String[] columns = new String[rsmd.getColumnCount()];
          for (int col = 0; col < columns.length; col++) {
            columns[col] = rsmd.getColumnLabel(col + 1);
          }
          
          while (rs.next()) {
            // Start from 1, because 0 is the row ID
            final String rowID = rs.getString(1);
            for(int col=1; col<columns.length; col++) {
              result.put(rowID, columns[col], rs.getObject(col+1));
            }
          }
        }
        return Tables.unmodifiableTable(result);
      }
    } catch (final SQLException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  
  /**
   * Selects a 3-column table and interprets it as "row, column, value"
   * @param sql
   * @param args
   * @return
   */
  public Table<?, ?, ?> selectTableRCV(final String sql, final Object... args) {
    Preconditions.checkNotNull(sql);
    Preconditions.checkArgument(sql.toLowerCase().contains("select"));
    try {
      final Table<Object, Object, Object> result = HashBasedTable.create();
      try (final PreparedStatement pstmt = conn.prepareStatement(sql);) {
        for (int i = 0; i < args.length; i++) {
          pstmt.setObject(i + 1, args[i]);
        }
        try (final ResultSet rs = pstmt.executeQuery()) {
          while (rs.next()) {
            // TODO: Pass in optional mapping functions to decode to date, etc.
            result.put(rs.getObject(1), rs.getObject(2), rs.getObject(3));
          }
        }
        return Tables.unmodifiableTable(result);
      }
    } catch (final SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  public int update(final String sql, final Object... args) {
    Preconditions.checkNotNull(sql);
    try (final PreparedStatement pstmt = conn.prepareStatement(sql);) {
      for (int i = 0; i < args.length; i++) {
        pstmt.setObject(i + 1, args[i]);
      }
      return pstmt.executeUpdate();
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
