/*
 * Copyright © 2015-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.common.db;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Hashtable;
import java.util.List;
import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * Utility methods for Database plugins shared by Database plugins.
 */
public final class DBUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);
  public static final String PLUGIN_TYPE_JDBC = "jdbc";
  public static final String OVERRIDE_SCHEMA = "io.cdap.hydrator.db.override.schema";
  public static final String PATTERN_TO_REPLACE = "io.cdap.plugin.db.pattern.replace";
  public static final String REPLACE_WITH = "io.cdap.plugin.db.replace.with";
  public static final String CONNECTION_ARGUMENTS = "io.cdap.hydrator.db.connection.arguments";
  public static final String FETCH_SIZE = "io.cdap.hydrator.db.fetch.size";
  public static final String POSTGRESQL_TAG = "postgresql";
  public static final String POSTGRESQL_DEFAULT_SCHEMA = "public";

  /**
   * Performs any Database related cleanup
   *
   * @param driverClass the JDBC driver class
   */
  public static void cleanup(Class<? extends Driver> driverClass) {
    ClassLoader pluginClassLoader = driverClass.getClassLoader();
    if (pluginClassLoader == null) {
      // This could only be null if the classLoader is the Bootstrap/Primordial classloader. This should never be the
      // case since the driver class is always loaded from the plugin classloader.
      LOG.warn("PluginClassLoader is null. Cleanup not necessary.");
      return;
    }
    shutDownMySQLAbandonedConnectionCleanupThread(pluginClassLoader);
    unregisterOracleMBean(pluginClassLoader);
  }

  /**
   * Ensures that the JDBC Driver specified in configuration is available and can be loaded. Also registers it with
   * {@link DriverManager} if it is not already registered.
   */
  public static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> jdbcDriverClass,
    String connectionString, String jdbcPluginName, String jdbcPluginType)
    throws IllegalAccessException, InstantiationException, SQLException {

    try {
      DriverManager.getDriver(connectionString);
      return new DriverCleanup(null);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.
      LOG.debug("Plugin Type: {} and Plugin Name: {}; Driver Class: {} not found. Registering JDBC driver via shim {} ",
                jdbcPluginType, jdbcPluginName, jdbcDriverClass.getName(), JDBCDriverShim.class.getName());
      final JDBCDriverShim driverShim = new JDBCDriverShim(jdbcDriverClass.newInstance());
      try {
        DBUtils.deregisterAllDrivers(jdbcDriverClass);
      } catch (NoSuchFieldException | ClassNotFoundException e1) {
        LOG.error("Unable to deregister JDBC Driver class {}", jdbcDriverClass);
      }
      DriverManager.registerDriver(driverShim);
      return new DriverCleanup(driverShim);
    }
  }


  /**
   * Given the result set, get the metadata of the result set and return list of {@link
   * io.cdap.cdap.api.data.schema.Schema.Field}, where name of the field is same as column name and type of the field is
   * obtained using {@link DBUtils#getSchema(String, int, int, int, String, boolean)}
   *
   * @param resultsetSchema the schema from the db
   * @param schemaStr       schema string to override resultant schema
   * @return list of schema fields
   */
  public static List<Schema.Field> getSchemaFields(Schema resultsetSchema, @Nullable String schemaStr) {
    Schema schema;

    if (!Strings.isNullOrEmpty(schemaStr)) {
      try {
        schema = Schema.parseJson(schemaStr);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Unable to parse schema string '%s'.", schemaStr), e);
      }
      for (Schema.Field field : schema.getFields()) {
        Schema.Field resultsetField = resultsetSchema.getField(field.getName());
        if (resultsetField == null) {
          throw new IllegalArgumentException(
            String.format("Schema field '%s' is not present in input record.", field.getName()));
        }
        Schema resultsetFieldSchema =
          resultsetField.getSchema().isNullable() ? resultsetField.getSchema().getNonNullable() :
            resultsetField.getSchema();
        Schema simpleSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();

        if (!isCompatible(resultsetFieldSchema, simpleSchema)) {
          throw new IllegalArgumentException(String
            .format("Schema field '%s' has type '%s' but in input record " + "found type '%s'.", field.getName(),
              simpleSchema.getDisplayName(), resultsetFieldSchema.getDisplayName()));
        }
      }
      return schema.getFields();

    }
    return resultsetSchema.getFields();
  }

  private static boolean isCompatible(Schema resultSetSchema, Schema mappedSchema) {
    if (resultSetSchema.equals(mappedSchema)) {
      return true;
    }
    //allow mapping from string to datetime, values will be validated for format
    if (resultSetSchema.getType() == Schema.Type.STRING &&
      mappedSchema.getLogicalType() == Schema.LogicalType.DATETIME) {
      return true;
    }
    return false;
  }

  /**
   * Given the result set, get the metadata of the result set and return list of {@link
   * io.cdap.cdap.api.data.schema.Schema.Field}, where name of the field is same as column name and type of the field is
   * obtained using {@link DBUtils#getSchema(String, int, int, int, String, boolean)}
   *
   * @param resultSet result set of executed query
   * @return list of schema fields
   * @throws SQLException
   */
  public static List<Schema.Field> getOriginalSchema(ResultSet resultSet, Schema outputSchema) throws SQLException {
    return getSchemaFields(resultSet, null, null, outputSchema);
  }

  /**
   * Given the result set, get the metadata of the result set and return list of {@link
   * io.cdap.cdap.api.data.schema.Schema.Field}, where name of the field is same as column name and type of the field is
   * obtained using {@link DBUtils#getSchema(String, int, int, int, String, boolean)}
   *
   * @param resultSet        result set of executed query
   * @param patternToReplace the pattern to replace in the field name
   * @param replaceWith      the replacement value, if it is null, the pattern will be removed
   * @return list of schema fields
   * @throws SQLException
   */
  public static List<Schema.Field> getSchemaFields(ResultSet resultSet, @Nullable String patternToReplace,
    @Nullable String replaceWith, @Nullable Schema outputSchema) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      if (patternToReplace != null) {
        columnName = columnName.replaceAll(patternToReplace, replaceWith == null ? "" : replaceWith);
      }
      int columnSqlType = metadata.getColumnType(i);
      int columnSqlPrecision = metadata.getPrecision(i); // total number of digits
      int columnSqlScale = metadata.getScale(i); // digits after the decimal point
      String columnTypeName = metadata.getColumnTypeName(i);
      boolean isSigned = metadata.isSigned(i);
      boolean handleAsDecimal = false;
      if (columnSqlType == Types.DECIMAL || columnSqlType == Types.NUMERIC) {
        if (outputSchema == null) {
          handleAsDecimal = true;
        } else {
          Schema.Field field = outputSchema.getField(columnName);
          if (field != null) {
            Schema schema = field.getSchema();
            schema = schema.isNullable() ? schema.getNonNullable() : schema;
            handleAsDecimal = Schema.LogicalType.DECIMAL == schema.getLogicalType();
          }
        }
      }
      Schema columnSchema = getSchema(
        columnTypeName, columnSqlType, columnSqlPrecision, columnSqlScale, columnName, isSigned, handleAsDecimal);
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  /**
   * load the specified JDBC driver class
   *
   * @param configurer     the plugin configurer
   * @param jdbcPluginName the jdbc plugin name
   * @param jdbcPluginId   the unique id of this usage
   * @param collector      the failure collector
   * @return the loaded JDBC driver class
   */
  public static Class<? extends Driver> loadJDBCDriverClass(PluginConfigurer configurer, String jdbcPluginName,
                                                            String jdbcPluginType, String jdbcPluginId,
                                                            @Nullable FailureCollector collector) {
    Class<? extends Driver> jdbcDriverClass =
      configurer.usePluginClass(jdbcPluginType, jdbcPluginName, jdbcPluginId, PluginProperties.builder().build());
    if (jdbcDriverClass == null) {
      String error = String.format("Unable to load JDBC Driver class for plugin name '%s'.", jdbcPluginName);
      String action = String.format(
        "Ensure that plugin '%s' of type '%s' containing the driver has been deployed.", jdbcPluginName,
        jdbcPluginType);
      if (collector != null) {
        collector.addFailure(error, action).withConfigProperty(DBConnectorProperties.PLUGIN_JDBC_PLUGIN_NAME)
          .withPluginNotFound(jdbcPluginId, jdbcPluginName, jdbcPluginType);
      } else {
        throw new IllegalArgumentException(error + " " + action);
      }
    }
    return jdbcDriverClass;
  }

  /**
   * Get a CDAP schema from a given database column definition
   *
   * @param typeName        data source dependent type name, for a UDT the type name is fully qualified
   * @param sqlType         SQL type from java.sql.Types
   * @param precision       the number of total digits for numeric types
   * @param scale           the number of fractional digits for numeric types
   * @param columnName      the column name
   * @param handleAsDecimal whether to convert numeric types to decimal logical type
   * @return the converted CDAP schema
   */
  public static Schema getSchema(String typeName, int sqlType, int precision, int scale, String columnName,
                                 boolean handleAsDecimal) throws SQLException {
    return getSchema(typeName, sqlType, precision, scale, columnName, true, handleAsDecimal);
  }

  /**
   * Get a CDAP schema from a given database column definition
   * @param typeName  data source dependent type name, for a UDT the type name is fully qualified
   * @param sqlType   SQL type from java.sql.Types
   * @param precision the number of total digits for numeric types
   * @param scale     the number of fractional digits for numeric types
   * @param columnName the column name
   * @param isSigned whether the data type is signed or unsigned
   * @param handleAsDecimal whether to convert numeric types to decimal logical type
   * @return the converted CDAP schema
   */
  public static Schema getSchema(String typeName, int sqlType, int precision, int scale, String columnName,
                                 boolean isSigned, boolean handleAsDecimal) {
    // Type.STRING covers sql types - VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR
    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.ROWID:
        break;

      case Types.BOOLEAN:
      case Types.BIT:
        type = Schema.Type.BOOLEAN;
        break;

      case Types.TINYINT:
      case Types.SMALLINT:
        type = Schema.Type.INT;
        break;
      case Types.INTEGER:
        // SQL INT is 32 bit, thus only signed can be stored in int
        type = isSigned ? Schema.Type.INT : Schema.Type.LONG;
        break;

      case Types.BIGINT:
        //SQL BIGINT is 64 bit, thus signed can be stored in long without losing precision
        //or unsigned BIGINT is within the scope of signed long
        if (isSigned || precision < 19) {
          type = Schema.Type.LONG;
          break;
        } else {
          // by default scale is 0, big integer won't have any fraction part
          return Schema.decimalOf(precision);
        }

      case Types.REAL:
      case Types.FLOAT:
        type = Schema.Type.FLOAT;
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
        if (handleAsDecimal) {
          return Schema.decimalOf(precision, scale);
        } else {

          // if there are no digits after the point, use integer types
          //SQL DECIMAL can be 5 - 17 bytes, not all can be held in a long
          type = scale != 0 || precision >= 19 ? Schema.Type.DOUBLE :
            // with 10 digits we can represent 2^32 and LONG is required
            precision > 9 ? Schema.Type.LONG : Schema.Type.INT;
          break;
        }
      case Types.DOUBLE:
        type = Schema.Type.DOUBLE;
        break;

      case Types.DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case Types.TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case Types.TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        type = Schema.Type.BYTES;
        break;

      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.SQLXML:
      case Types.STRUCT:
        String errorMessage = String.format("Column %s has unsupported SQL type of %s.", columnName, typeName);
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          errorMessage, errorMessage, ErrorType.SYSTEM, true, null);
    }

    return Schema.of(type);
  }

  @Nullable
  public static Object transformValue(int sqlType, int precision, int scale, ResultSet resultSet, String fieldName,
    Schema outputFieldSchema) throws SQLException {
    Object original = resultSet.getObject(fieldName);
    if (original != null) {
      switch (sqlType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return ((Number) original).intValue();
        case Types.NUMERIC:
        case Types.DECIMAL: {
          if (Schema.LogicalType.DECIMAL == outputFieldSchema.getLogicalType()) {
            // It's required to pass 'scale' parameter since in the case of some dbs like Oracle, scale of 'BigDecimal'
            // depends on the scale of actual value. For example for value '77.12'
            // scale will be '2' even if sql scale is '6'
            return resultSet.getBigDecimal(fieldName, scale);
          } else {
            BigDecimal decimal = (BigDecimal) original;
            if (scale != 0) {
              // if there are digits after the point, use double types
              return decimal.doubleValue();
            } else if (precision > 9) {
              // with 10 digits we can represent 2^32 and LONG is required
              return decimal.longValue();
            } else {
              return decimal.intValue();
            }
          }
        }
        case Types.DATE:
          return resultSet.getDate(fieldName);
        case Types.TIME:
          return resultSet.getTime(fieldName);
        case Types.TIMESTAMP:
          return resultSet.getTimestamp(fieldName);
        case Types.ROWID:
          return resultSet.getString(fieldName);
        case Types.BLOB:
          Blob blob = (Blob) original;
          try {
            return blob.getBytes(1, (int) blob.length());
          } finally {
            blob.free();
          }
        case Types.CLOB:
          Clob clob = (Clob) original;
          try {
            return clob.getSubString(1, (int) clob.length());
          } finally {
            clob.free();
          }
      }
    }
    return original;
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  public static void deregisterAllDrivers(Class<? extends Driver> driverClass)
    throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    Field field = DriverManager.class.getDeclaredField("registeredDrivers");
    field.setAccessible(true);
    List<?> list = (List<?>) field.get(null);
    for (Object driverInfo : list) {
      Class<?> driverInfoClass = DBUtils.class.getClassLoader().loadClass("java.sql.DriverInfo");
      Field driverField = driverInfoClass.getDeclaredField("driver");
      driverField.setAccessible(true);
      Driver d = (Driver) driverField.get(driverInfo);
      if (d == null) {
        LOG.debug("Found null driver object in drivers list. Ignoring.");
        continue;
      }
      LOG.debug("Removing non-null driver object from drivers list.");
      ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
      if (registeredDriverClassLoader == null) {
        LOG.debug("Found null classloader for default driver {}. Ignoring since this may be using system classloader.",
          d.getClass().getName());
        continue;
      }
      // Remove all objects in this list that were created using the classloader of the caller.
      if (d.getClass().getClassLoader().equals(driverClass.getClassLoader())) {
        LOG.debug("Removing default driver {} from registeredDrivers", d.getClass().getName());
        list.remove(driverInfo);
      }
    }
  }

  /**
   * Shuts down a cleanup thread com.mysql.jdbc.AbandonedConnectionCleanupThread that mysql driver fails to destroy If
   * this is not done, the thread keeps a reference to the classloader, thereby causing OOMs or too many open files
   *
   * @param classLoader the unfiltered classloader of the jdbc driver class
   */
  private static void shutDownMySQLAbandonedConnectionCleanupThread(ClassLoader classLoader) {
    try {
      Class<?> mysqlCleanupThreadClass;
      try {
        mysqlCleanupThreadClass = classLoader.loadClass("com.mysql.jdbc.AbandonedConnectionCleanupThread");
      } catch (ClassNotFoundException e) {
        // Ok to ignore, since we may not be running mysql
        LOG.trace("Failed to load MySQL abandoned connection cleanup thread class. Presuming DB App is " +
          "not being run with MySQL and ignoring", e);
        return;
      }
      Method shutdownMethod = mysqlCleanupThreadClass.getMethod("shutdown");
      shutdownMethod.invoke(null);
      LOG.debug("Successfully shutdown MySQL connection cleanup thread.");
    } catch (Throwable e) {
      // cleanup failed, ignoring silently with a log, since not much can be done.
      LOG.warn("Failed to shutdown MySQL connection cleanup thread. Ignoring.", e);
    }
  }

  private static void unregisterOracleMBean(ClassLoader classLoader) {
    try {
      classLoader.loadClass("oracle.jdbc.driver.OracleDriver");
    } catch (ClassNotFoundException e) {
      LOG.debug("Oracle JDBC Driver not found. Presuming that the DB App is not being run with an Oracle DB. " +
                  "Not attempting to cleanup Oracle MBean.");
      return;
    }
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Hashtable<String, String> keys = new Hashtable<>();
    keys.put("type", "diagnosability");
    keys.put(
      "name", classLoader.getClass().getName() + "@" + Integer.toHexString(classLoader.hashCode()).toLowerCase());
    ObjectName oracleJdbcMBeanName;
    try {
      oracleJdbcMBeanName = new ObjectName("com.oracle.jdbc", keys);
    } catch (MalformedObjectNameException e) {
      // This should never happen, since we're constructing the ObjectName correctly
      LOG.debug("Exception while constructing Oracle JDBC MBean Name. Aborting cleanup.", e);
      return;
    }
    try {
      mbs.getMBeanInfo(oracleJdbcMBeanName);
    } catch (InstanceNotFoundException e) {
      LOG.debug("Oracle JDBC MBean not found. No cleanup necessary.");
      return;
    } catch (IntrospectionException | ReflectionException e) {
      LOG.debug("Exception while attempting to retrieve Oracle JDBC MBean. Aborting cleanup.", e);
      return;
    }

    try {
      mbs.unregisterMBean(oracleJdbcMBeanName);
      LOG.debug("Oracle MBean unregistered successfully.");
    } catch (InstanceNotFoundException | MBeanRegistrationException e) {
      LOG.debug("Exception while attempting to cleanup Oracle JDBCMBean. Aborting cleanup.", e);
    }
  }

  private DBUtils() {
    String errorMessage = "Should not instantiate static utility class.";
    throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage,
      errorMessage, ErrorType.SYSTEM, false, null);
  }
}
