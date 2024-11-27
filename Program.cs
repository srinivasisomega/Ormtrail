using System.Data;
using System.Reflection;
using System.Text;
using Microsoft.Data.SqlClient;
namespace Orm
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public string Name { get; }
        public TableAttribute(string name) => Name = name;
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public string Name { get; }
        public bool IsPrimaryKey { get; }
        public ColumnAttribute(string name, bool isPrimaryKey = false)
        {
            Name = name;
            IsPrimaryKey = isPrimaryKey;
        }
    }

    public class DbContext
    {
        private readonly string _connectionString;

        public DbContext(string connectionString)
        {
            _connectionString = connectionString;
        }
        public void CreateTable<T>() where T : class
        {
            var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
            if (tableAttribute == null)
                throw new InvalidOperationException($"Class {typeof(T).Name} is not mapped to a table.");

            string tableName = tableAttribute.Name;
            var properties = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<ColumnAttribute>() != null);

            var columns = properties.Select(p =>
            {
                var column = p.GetCustomAttribute<ColumnAttribute>();
                string columnDef = $"{column.Name} {GetSqlType(p.PropertyType)}";

                // Check if the column is marked as primary key and ensure it's not nullable
                if (column.IsPrimaryKey)
                {
                    columnDef += " NOT NULL";  // Primary key should be non-nullable
                }

                if (column.IsPrimaryKey) columnDef += " PRIMARY KEY";  // Add primary key constraint
                return columnDef;
            });

            string createTableSql = $"CREATE TABLE dbo.{tableName} ({string.Join(", ", columns)});";

            Console.WriteLine($"SQL to create table: {createTableSql}");  // Log the SQL query for debugging

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(createTableSql, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        // Add the GetSqlType method to map C# types to SQL types
        private string GetSqlType(Type type)
        {
            if (type == typeof(int)) return "INT";
            if (type == typeof(string)) return "NVARCHAR(MAX)";
            if (type == typeof(bool)) return "BIT";
            if (type == typeof(DateTime)) return "DATETIME";
            // Add more type mappings as needed

            throw new NotSupportedException($"Type {type.Name} is not supported.");
        }
    

    public void GenerateModelsFromDatabase(string outputPath)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var schema = connection.GetSchema("Tables");

                foreach (DataRow row in schema.Rows)
                {
                    string tableName = row["TABLE_NAME"].ToString();

                    // Query column details
                    string columnQuery = $"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'";
                    var columns = new List<(string Name, string Type)>();

                    using (var command = new SqlCommand(columnQuery, connection))
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string columnName = reader.GetString(0);
                            string dataType = reader.GetString(1);
                            columns.Add((columnName, dataType));
                        }
                    }

                    // Generate C# class code
                    var classCode = GenerateClassCode(tableName, columns);

                    // Save to file
                    File.WriteAllText(Path.Combine(outputPath, $"{tableName}.cs"), classCode);
                }
            }
        }

        private string GenerateClassCode(string tableName, List<(string Name, string Type)> columns)
        {
            var sb = new StringBuilder();
            sb.AppendLine("using System;");
            sb.AppendLine("using Orm;");
            sb.AppendLine();
            sb.AppendLine($"[Table(\"{tableName}\")]");
            sb.AppendLine($"public class {tableName}");
            sb.AppendLine("{");

            foreach (var column in columns)
            {
                sb.AppendLine($"    [Column(\"{column.Name}\")]");
                sb.AppendLine($"    public {MapSqlTypeToCSharp(column.Type)} {column.Name} {{ get; set; }}");
            }

            sb.AppendLine("}");
            return sb.ToString();
        }
        private string MapSqlTypeToCSharp(string sqlType)
        {
            return sqlType.ToLower() switch
            {
                "int" => "int",
                "nvarchar" => "string",
                "bit" => "bool",
                "datetime" => "DateTime",
                "decimal" => "decimal",
                "float" => "float",
                "bigint" => "long",
                "guid" => "Guid",
                _ => throw new NotSupportedException($"SQL type {sqlType} is not supported.")
            };
        }
        public void Insert<T>(T entity)
        {
            var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
            if (tableAttribute == null)
                throw new InvalidOperationException($"Class {typeof(T).Name} is not mapped to a table.");

            string tableName = tableAttribute.Name;
            var properties = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<ColumnAttribute>() != null);

            var columnNames = properties.Select(p => p.GetCustomAttribute<ColumnAttribute>().Name);
            var parameterNames = properties.Select(p => $"@{p.Name}");

            string insertSql = $"INSERT INTO {tableName} ({string.Join(", ", columnNames)}) VALUES ({string.Join(", ", parameterNames)})";

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(insertSql, connection))
                {
                    foreach (var property in properties)
                    {
                        command.Parameters.AddWithValue($"@{property.Name}", property.GetValue(entity) ?? DBNull.Value);
                    }
                    command.ExecuteNonQuery();
                }
            }
        }
        public void Delete<T>(T entity)
        {
            var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
            if (tableAttribute == null)
                throw new InvalidOperationException($"Class {typeof(T).Name} is not mapped to a table.");

            string tableName = tableAttribute.Name;
            var properties = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<ColumnAttribute>() != null)
                .ToList();

            // Identify the primary key property
            var primaryKeyProperty = properties.FirstOrDefault(p => p.GetCustomAttribute<ColumnAttribute>()?.IsPrimaryKey == true);
            if (primaryKeyProperty == null)
                throw new InvalidOperationException($"Class {typeof(T).Name} does not have a primary key.");

            // Get primary key column name for the WHERE clause
            var primaryKeyColumn = primaryKeyProperty.GetCustomAttribute<ColumnAttribute>().Name;

            // Generate SQL delete query
            string deleteSql = $"DELETE FROM {tableName} WHERE {primaryKeyColumn} = @{primaryKeyProperty.Name}";

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(deleteSql, connection))
                {
                    // Add primary key parameter to the WHERE clause
                    command.Parameters.AddWithValue($"@{primaryKeyProperty.Name}", primaryKeyProperty.GetValue(entity));

                    command.ExecuteNonQuery();
                }
            }
        }

        public void Update<T>(T entity)
        {
            var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
            if (tableAttribute == null)
                throw new InvalidOperationException($"Class {typeof(T).Name} is not mapped to a table.");

            string tableName = tableAttribute.Name;
            var properties = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<ColumnAttribute>() != null)
                .ToList();

            // Identify the primary key property
            var primaryKeyProperty = properties.FirstOrDefault(p => p.GetCustomAttribute<ColumnAttribute>()?.IsPrimaryKey == true);
            if (primaryKeyProperty == null)
                throw new InvalidOperationException($"Class {typeof(T).Name} does not have a primary key.");

            // Get column names excluding the primary key for the SET clause
            var setClauses = properties
                .Where(p => p != primaryKeyProperty)
                .Select(p => $"{p.GetCustomAttribute<ColumnAttribute>().Name} = @{p.Name}")
                .ToList();

            // Get the primary key column name for the WHERE clause
            var primaryKeyColumn = primaryKeyProperty.GetCustomAttribute<ColumnAttribute>().Name;

            // Generate SQL update query
            string updateSql = $"UPDATE {tableName} SET {string.Join(", ", setClauses)} WHERE {primaryKeyColumn} = @{primaryKeyProperty.Name}";

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(updateSql, connection))
                {
                    // Add parameters for each property (excluding the primary key)
                    foreach (var property in properties)
                    {
                        if (property != primaryKeyProperty)
                        {
                            command.Parameters.AddWithValue($"@{property.Name}", property.GetValue(entity) ?? DBNull.Value);
                        }
                    }

                    // Add primary key parameter to the WHERE clause
                    command.Parameters.AddWithValue($"@{primaryKeyProperty.Name}", primaryKeyProperty.GetValue(entity));

                    command.ExecuteNonQuery();
                }
            }
        }
        public void MigrateDatabase<T>() where T : class
        {
            var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
            if (tableAttribute == null)
                throw new InvalidOperationException($"Class {typeof(T).Name} is not mapped to a table.");

            string tableName = tableAttribute.Name;

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                // Get existing table schema
                var existingColumns = GetExistingColumns(tableName, connection);

                // Get model properties
                var modelProperties = typeof(T).GetProperties()
                    .Where(p => p.GetCustomAttribute<ColumnAttribute>() != null)
                    .ToList();

                // Generate migration SQL
                var migrationSql = GenerateMigrationSql(tableName, existingColumns, modelProperties);

                // Execute migration SQL
                if (!string.IsNullOrWhiteSpace(migrationSql))
                {
                    using (var command = new SqlCommand(migrationSql, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        private List<(string Name, string Type)> GetExistingColumns(string tableName, SqlConnection connection)
        {
            var columns = new List<(string Name, string Type)>();
            string columnQuery = $"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'";

            using (var command = new SqlCommand(columnQuery, connection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    columns.Add((reader.GetString(0), reader.GetString(1)));
                }
            }

            return columns;
        }

        private string GenerateMigrationSql(string tableName, List<(string Name, string Type)> existingColumns, List<PropertyInfo> modelProperties)
        {
            var modelColumns = modelProperties
                .Select(p => (Name: p.GetCustomAttribute<ColumnAttribute>().Name, Type: GetSqlType(p.PropertyType), IsPrimaryKey: p.GetCustomAttribute<ColumnAttribute>().IsPrimaryKey))
                .ToList();

            var addColumns = modelColumns
                .Where(mc => !existingColumns.Any(ec => ec.Name == mc.Name))
                .Select(mc => $"ALTER TABLE {tableName} ADD {mc.Name} {mc.Type}");

            var dropColumns = existingColumns
                .Where(ec => !modelColumns.Any(mc => mc.Name == ec.Name))
                .Select(ec => $"ALTER TABLE {tableName} DROP COLUMN {ec.Name}");

            var modifyColumns = modelColumns
                .Where(mc => existingColumns.Any(ec => ec.Name == mc.Name && ec.Type != mc.Type))
                .Select(mc =>
                {
                    var primaryKeyDrop = mc.IsPrimaryKey ? $"ALTER TABLE {tableName} DROP CONSTRAINT PK_{tableName}; " : "";
                    return $"{primaryKeyDrop}ALTER TABLE {tableName} ALTER COLUMN {mc.Name} {mc.Type}";
                });

            var primaryKeyCommands = modelColumns
                .Where(mc => mc.IsPrimaryKey)
                .Select(mc => $"ALTER TABLE {tableName} ADD CONSTRAINT PK_{tableName} PRIMARY KEY ({mc.Name})");

            return string.Join("; ", addColumns.Concat(dropColumns).Concat(modifyColumns).Concat(primaryKeyCommands));
        }


    }
}