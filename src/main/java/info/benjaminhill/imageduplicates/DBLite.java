package info.benjaminhill.imageduplicates;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.sqlite.SQLiteConnection;
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

  public boolean exists() {
    return new File(FILE_NAME).canRead();
  }

  public long selectLong(final String sql, final Object... args) throws SQLException {
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
    }
  }

  public Table<?, ?, ?> selectTable(final String sql, final Object... args) {
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

  public int update(final String sql, final Object... args) throws SQLException {
    try (final PreparedStatement pstmt = conn.prepareStatement(sql);) {
      for (int i = 0; i < args.length; i++) {
        pstmt.setObject(i + 1, args[i]);
      }
      return pstmt.executeUpdate();
    }
  }

  @Override
  public void close() throws Exception {
    this.conn.close();
  }
}
