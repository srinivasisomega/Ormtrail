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
            if (Nullable.GetUnderlyingType(type) != null)
                type = Nullable.GetUnderlyingType(type);

            return type switch
            {
                var t when t == typeof(int) => "INT",
                var t when t == typeof(long) => "BIGINT",
                var t when t == typeof(decimal) => "DECIMAL(18,2)",
                var t when t == typeof(double) => "FLOAT",
                var t when t == typeof(string) => "NVARCHAR(MAX)",
                var t when t == typeof(bool) => "BIT",
                var t when t == typeof(DateTime) => "DATETIME",
                _ => throw new InvalidOperationException($"Unsupported type {type.Name}")
            };
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
        private string MapClrTypeToSqlType(Type clrType)
        {
            if (clrType == typeof(int))
                return "INT";
            if (clrType == typeof(string))
                return "NVARCHAR(MAX)";
            if (clrType == typeof(DateTime))
                return "DATETIME";
            if (clrType == typeof(bool))
                return "BIT";
            if (clrType == typeof(decimal) || clrType == typeof(double) || clrType == typeof(float))
                return "DECIMAL";

            throw new NotSupportedException($"Unsupported CLR type: {clrType.FullName}");
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
                    .ToList(); // Keep as List<PropertyInfo>

                // Generate migration SQL
                var migrationSql = GenerateMigrationSql(tableName, existingColumns, modelProperties, connection);

                // Handle foreign keys if altering a primary key
                var primaryKeyProperty = modelProperties.FirstOrDefault(p => p.GetCustomAttribute<ColumnAttribute>()?.IsPrimaryKey == true);

                if (primaryKeyProperty != null)
                {
                    var primaryKeyColumn = primaryKeyProperty.GetCustomAttribute<ColumnAttribute>().Name;
                    var foreignKeyCommands = new List<string>();
                    HandleForeignKeys(tableName, primaryKeyColumn, connection, foreignKeyCommands);

                    // Execute foreign key commands
                    foreach (var cmd in foreignKeyCommands)
                    {
                        using (var command = new SqlCommand(cmd, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                }

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


        private List<string> CompareSchema(string tableName, List<(string Name, string Type, bool IsNullable)> existingColumns, List<PropertyInfo> modelProperties, SqlConnection connection)
        {
            var modelColumns = modelProperties.Select(p => new
            {
                Name = p.GetCustomAttribute<ColumnAttribute>().Name,
                Type = GetSqlType(p.PropertyType),
                IsNullable = !p.PropertyType.IsValueType || Nullable.GetUnderlyingType(p.PropertyType) != null,
                IsPrimaryKey = p.GetCustomAttribute<ColumnAttribute>().IsPrimaryKey
            }).ToList();

            var commands = new List<string>();

            // Detect missing or altered columns
            foreach (var modelColumn in modelColumns)
            {
                var existingColumn = existingColumns.FirstOrDefault(c => c.Name == modelColumn.Name);

                if (existingColumn == default)
                {
                    // Column missing in database
                    commands.Add($"ALTER TABLE {tableName} ADD {modelColumn.Name} {modelColumn.Type} {(modelColumn.IsNullable ? "NULL" : "NOT NULL")}");
                }
                else if (existingColumn.Type != modelColumn.Type || existingColumn.IsNullable != modelColumn.IsNullable)
                {
                    // Column type mismatch or nullability mismatch
                    commands.Add($"ALTER TABLE {tableName} ALTER COLUMN {modelColumn.Name} {modelColumn.Type} {(modelColumn.IsNullable ? "NULL" : "NOT NULL")}");
                }
            }

            // Detect columns in database but not in the model
            foreach (var existingColumn in existingColumns)
            {
                if (!modelColumns.Any(mc => mc.Name == existingColumn.Name))
                {
                    // Drop column (requires constraints to be handled)
                    commands.Add($"ALTER TABLE {tableName} DROP COLUMN {existingColumn.Name}");
                }
            }

            return commands;
        }
        private List<(string Name, string Type, bool IsNullable)> GetExistingColumns(string tableName, SqlConnection connection)
        {
            var existingColumns = new List<(string Name, string Type, bool IsNullable)>();

            string query = $@"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName";

            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@TableName", tableName);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader["COLUMN_NAME"].ToString();
                        string columnType = reader["DATA_TYPE"].ToString();
                        bool isNullable = reader["IS_NULLABLE"].ToString() == "YES";

                        existingColumns.Add((columnName, columnType, isNullable));
                    }
                }
            }

            return existingColumns;
        }
        private List<(string ConstraintName, string Type)> GetConstraints(string tableName, SqlConnection connection)
        {
            var constraints = new List<(string ConstraintName, string Type)>();
            string query = @"
        SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
        WHERE TABLE_NAME = @TableName";

            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@TableName", tableName);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        constraints.Add((reader.GetString(0), reader.GetString(1)));
                    }
                }
            }
            return constraints;
        }

        private (string ConstraintName, string ColumnName) GetPrimaryKeyConstraint(string tableName, SqlConnection connection)
        {
            var query = @"
        SELECT 
            kcu.COLUMN_NAME, 
            tc.CONSTRAINT_NAME 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.TABLE_NAME = @TableName AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'";

            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@TableName", tableName);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return (reader["CONSTRAINT_NAME"].ToString(), reader["COLUMN_NAME"].ToString());
                    }
                }
            }

            return (null, null); // No primary key found
        }
        private string GenerateMigrationSql(string tableName, List<(string Name, string Type, bool IsNullable)> existingColumns, List<PropertyInfo> modelProperties, SqlConnection connection)
        {
            var modelColumns = modelProperties
                .Select(p => (
                    Name: p.GetCustomAttribute<ColumnAttribute>().Name,
                    Type: GetSqlType(p.PropertyType),
                    IsPrimaryKey: p.GetCustomAttribute<ColumnAttribute>().IsPrimaryKey,
                    IsNullable: !p.PropertyType.IsValueType || Nullable.GetUnderlyingType(p.PropertyType) != null
                ))
                .ToList();

            var commands = new List<string>();

            // Step 1: Drop foreign key constraints if altering primary key or column dependencies
            var (constraintName, primaryKeyColumn) = GetPrimaryKeyConstraint(tableName, connection);
            if (!string.IsNullOrEmpty(constraintName))
            {
                commands.Add($"ALTER TABLE {tableName} DROP CONSTRAINT {constraintName}");
            }

            var foreignKeyCommands = new List<string>();
            foreach (var modelColumn in modelColumns)
            {
                if (modelColumn.IsPrimaryKey)
                {
                    HandleForeignKeys(tableName, modelColumn.Name, connection, foreignKeyCommands);
                }
            }
            commands.AddRange(foreignKeyCommands);

            // Step 2: Alter existing columns or add missing ones
            foreach (var modelColumn in modelColumns)
            {
                var existingColumn = existingColumns.FirstOrDefault(ec => ec.Name == modelColumn.Name);
                if (existingColumn != default)
                {
                    // Update column type or nullability
                    if (existingColumn.Type != modelColumn.Type || existingColumn.IsNullable != modelColumn.IsNullable)
                    {
                        commands.Add($"ALTER TABLE {tableName} ALTER COLUMN {modelColumn.Name} {modelColumn.Type} {(modelColumn.IsNullable ? "NULL" : "NOT NULL")}");
                    }
                }
                else
                {
                    // Add missing column
                    commands.Add($"ALTER TABLE {tableName} ADD {modelColumn.Name} {modelColumn.Type} {(modelColumn.IsNullable ? "NULL" : "NOT NULL")}");
                }
            }

            // Step 3: Recreate primary key
            if (!string.IsNullOrEmpty(primaryKeyColumn))
            {
                commands.Add($"ALTER TABLE {tableName} ADD CONSTRAINT PK_{tableName} PRIMARY KEY ({primaryKeyColumn})");
            }

            // Step 4: Recreate foreign key constraints
            foreach (var fkCommand in foreignKeyCommands)
            {
                commands.Add(fkCommand);
            }

            return string.Join("; ", commands);
        }

        private void HandleForeignKeys(string tableName, string columnName, SqlConnection connection, List<string> commands)
        {
            string query = @"SELECT rc.CONSTRAINT_NAME, fk.TABLE_NAME, fk.COLUMN_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME WHERE rc.UNIQUE_CONSTRAINT_NAME = (
    SELECT CONSTRAINT_NAME 
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    WHERE TABLE_NAME = @TableName AND CONSTRAINT_TYPE = 'PRIMARY KEY'
)";

            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@TableName", tableName);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string fkConstraint = reader["CONSTRAINT_NAME"].ToString();
                        string fkTable = reader["TABLE_NAME"].ToString();
                        string fkColumn = reader["COLUMN_NAME"].ToString();

                        // Drop the foreign key constraint
                        commands.Add($"ALTER TABLE {fkTable} DROP CONSTRAINT {fkConstraint}");

                        // Save the constraint for recreation
                        commands.Add($"ALTER TABLE {fkTable} ADD CONSTRAINT {fkConstraint} FOREIGN KEY ({fkColumn}) REFERENCES {tableName} ({columnName})");
                    }
                }
            }
        }


    }
}