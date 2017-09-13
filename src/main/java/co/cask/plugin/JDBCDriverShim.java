package co.cask.plugin;


import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Shim for JDBC driver as a better alternative to mere Class.forName to load the JDBC Driver class.
 *
 * From http://www.kfu.com/~nsayer/Java/dyn-jdbc.html
 * One problem with using <pre>{@code Class.forName()}</pre> to find and load the JDBC Driver class is that it
 * presumes that your driver is in the classpath. This means either packaging the driver in your jar, or having to
 * stick the driver somewhere (probably unpacking it too), or modifying your classpath.
 * But why not use something like URLClassLoader and the overload of Class.forName() that lets you specify the
 * ClassLoader?" Because the DriverManager will refuse to use a driver not loaded by the system ClassLoader.
 * The workaround for this is to create a shim class that implements java.sql.Driver.
 * This shim class will do nothing but call the methods of an instance of a JDBC driver that we loaded dynamically.
 */
public class JDBCDriverShim implements Driver {

  private final Driver delegate;

  public JDBCDriverShim(Driver delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return delegate.acceptsURL(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return delegate.connect(url, info);
  }

  @Override
  public int getMajorVersion() {
    return delegate.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return delegate.getMinorVersion();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return delegate.getPropertyInfo(url, info);
  }

  @Override
  public boolean jdbcCompliant() {
    return delegate.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegate.getParentLogger();
  }
}
