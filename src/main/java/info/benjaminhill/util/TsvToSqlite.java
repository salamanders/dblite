package info.benjaminhill.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Joiner;

public class TsvToSqlite {

	private static final Logger LOG = Logger.getLogger(TsvToSqlite.class.getName());
	private static String delim = "\t";

	private static String sanitize(final String in) {
		return in.toLowerCase().replaceAll("-", "_").replaceAll("[^a-z0-9_]", "");
	}

	/**
	 * Sanitize the names and wrap in back-ticks
	 *
	 * @param cols
	 * @return
	 */
	private static String colsToQuoted(final String... cols) {
		final List<String> colNames = new ArrayList<>();
		for (final String colName : cols) {
			colNames.add("`" + sanitize(colName) + "`");
		}
		return Joiner.on(',').join(colNames);
	}

	private static String colsToQmarks(final String... cols) {
		final List<String> colNames = new ArrayList<>();
		for (int i = 0; i < cols.length; i++) {
			colNames.add("?");
		}
		return Joiner.on(',').join(colNames);
	}
	
	private static void setLogging() {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s%n");
		
		final Logger logger = Logger.getAnonymousLogger();
    logger.setLevel(Level.ALL);

    final ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);
	}

	/**
	 * @param args
	 *          name of file
	 */
	public static void main(final String... args) throws IOException, SQLException {
		setLogging();
		
		Files.list(Paths.get(".")).filter(Files::isReadable).filter(path -> {
			return "tsv".equals(com.google.common.io.Files.getFileExtension(path.toString()).toLowerCase());
		}).forEach(path -> {
			final String tableName = sanitize(com.google.common.io.Files.getNameWithoutExtension(path.getFileName()
					.toString()));
			LOG.info("Converting:" + path.toString() + " to " + tableName);

			try {
				TsvToSqlite.insertBulkTsv(tableName, path.toFile().getCanonicalPath());
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		});

	}

	private static String getSQLInsert(final String tableName, final String... cols) {
		return String.format("INSERT INTO `%s` (%s) VALUES (%s)", tableName, colsToQuoted(cols), colsToQmarks(cols));
	}

	/**
	 * @param tableName
	 * @param dmls
	 *          the first is part of the primary key, others are optional
	 * @return
	 */
	private static String getSQLCreate(final String tableName, final String[] cols, final String[] colTypes) {
		assert cols.length == colTypes.length;
		final StringBuilder result = new StringBuilder();
		result.append(String.format("CREATE TABLE %s ", tableName));
		result.append("(");

		for (int i = 0; i < cols.length; i++) {
			result.append(cols[i]).append(" ").append(colTypes[i]);
			if (i < cols.length - 1) {
				result.append(",");
			}
		}
		result.append(")");
		return result.toString();
	}


	public static void insertBulkTsv(final String tableName, final String fileName) throws IOException, SQLException {
		LOG.log(Level.INFO, "Table {0}", tableName);
		LOG.log(Level.INFO, "File {0}", fileName);

		final File f = new File(fileName);
		
		// Figure out the columns
		String line;
		try (final BufferedReader b = new BufferedReader(new FileReader(f))) {
			final String[] cols = b.readLine().trim().split(delim);
			final int numCols = cols.length;
			b.mark(1024 * 1024);
			final String[] colTypes = new String[numCols];
			while ((line = b.readLine()) != null) {
				if (line.trim().isEmpty() || line.startsWith("#")) {
					continue;
				}
				final String[] lineData = line.split(delim);
				if (numCols != lineData.length) {
					continue;
				}
				for (int i = 0; i < numCols; i++) {
					String colType = "TEXT";
					try {
						Double.parseDouble(lineData[i]);
						colType = "REAL";
						Integer.parseInt(lineData[i]);
						colType = "INTEGER";
					} catch (final NumberFormatException nfe) {
						// ignore.
					}
					colTypes[i] = colType;
				}
				break;
			}
			b.reset();

			final String sqlCreate = getSQLCreate(tableName, cols, colTypes);
			final String sqlInsert = getSQLInsert(tableName, cols);

			LOG.log(Level.INFO, "Bulk insert sqlCreate:{0}", sqlCreate);
			LOG.log(Level.INFO, "Bulk insert sqlInsert:{0}", sqlInsert);

			try {
				DBLite.DB.update(sqlCreate);
			} catch (final Exception ex) {
				throw new RuntimeException("Table likely already existed", ex);
			}
			long queuedInserts = 0, totalRows = 0, totalLines=0;

			try (final PreparedStatement ps = DBLite.DB.conn.prepareStatement(sqlInsert)) {
				while ((line = b.readLine()) != null) {
					totalLines++;
					if (line.trim().isEmpty() || line.startsWith("#")) {
						continue;
					}
					final String[] lineData = line.split(delim);
					if (numCols != lineData.length) {
						LOG.log(Level.WARNING, "Line mismatch:{0},{1}", new Object[] { numCols, lineData.length });
						continue;
					}
					for (int i = 0; i < numCols; i++) {
						ps.setString(i + 1, lineData[i]);
					}
					ps.addBatch();
					queuedInserts++;
					totalRows++;

					if (queuedInserts >= 100_000) {
						LOG.log(Level.FINER, "Bulk insert:{0}", queuedInserts);
						LOG.log(Level.FINE, "Total :{0}", totalRows);
						queuedInserts = 0;
						ps.executeBatch();
						ps.clearBatch();
					}
				}
				
				LOG.log(Level.INFO, "Finished reading file: {0}", totalLines);
				if (queuedInserts > 0) {
					LOG.log(Level.FINER, "Final bulk insert: {0}", queuedInserts);
					LOG.log(Level.FINE, "Total: {0}", totalRows);
					ps.executeBatch();
					ps.clearBatch();
				}
				ps.close();
				// conn.createStatement().execute("PRAGMA auto_vacuum = 1");
				// conn.createStatement().execute("VACUUM");
			}
		}
	}

}
