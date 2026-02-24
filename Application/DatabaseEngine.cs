namespace RAMBaseDB.Application;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using RAMBaseDB.Domain.Abstractions;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Metadata;
using RAMBaseDB.Domain.Schema;
using RAMBaseDB.Domain.ValueObjects;
using RAMBaseDB.Infrastructure.Configuration;

/// <summary>
/// Provides in-memory management of multiple databases and their tables, supporting creation, configuration, data
/// manipulation, and persistence operations. Enables loading and saving databases to disk, table-level operations, and
/// configuration-based management for snapshot retention and restoration.
/// </summary>
/// <remarks>DatabaseManager is designed for scenarios requiring lightweight, in-memory database structures with
/// optional persistence to compressed JSON files. It supports concurrent access to database configurations and provides
/// thread safety for operations that modify database or table state. Use this class to programmatically create,
/// configure, and manipulate databases and tables, as well as to serialize and restore their contents. Dispose the
/// instance to release all managed resources and clear in-memory data. This class is not intended for use as a
/// general-purpose relational database engine; it is optimized for application-level data storage and
/// retrieval.</remarks>
public class DatabaseEngine : IDisposable
{
    // Root structure: DatabaseName -> (TableName -> Table instance)
    private readonly ConcurrentDictionary<string, DatabaseConfiguration> _configurations;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Dictionary<string, Database> _databases;

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };
    private const string MetadataAssemblyName = "RAMBaseDB.MetadataTables";
    private const string MetadataNamespacePrefix = "RAMBaseDB.Metadata.";

    public DatabaseEngine()
    {
        _databases = new Dictionary<string, Database>(StringComparer.Ordinal);
        _configurations = new();
        _lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
    }

    public DatabaseEngine(Database database)
        : this()
    {
        ArgumentNullException.ThrowIfNull(database);

        if (string.IsNullOrWhiteSpace(database.Name))
            throw new ArgumentException("Database name cannot be empty.", nameof(database));

        var normalizedName = NormalizeDatabaseName(database.Name, nameof(database.Name));
        _databases[normalizedName] = database;
    }

    /// <summary>
    /// Creates a new database in memory (if it doesn't exist).
    /// </summary>
    public void CreateDatabase(string dbName)
    {
        var normalizedName = NormalizeDatabaseName(dbName, nameof(dbName));
        ExecuteWithWriteLock(() => CreateDatabaseUnsafe(normalizedName));
    }


    /// <summary>
    /// Creates a database using the supplied configuration metadata.
    /// </summary>
    public void CreateDatabase(DatabaseConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        configuration.Validate();
        var normalizedName = NormalizeDatabaseName(configuration.DataConfig.DatabaseName, nameof(configuration.DataConfig.DatabaseName));
        _configurations[normalizedName] = configuration;
        if (configuration.DataConfig.AutoRestoreLatestDump && TryLoadLatestDump(configuration))
            return;

        ExecuteWithWriteLock(() => CreateDatabaseUnsafe(normalizedName));
        CreateDatabaseMetadata(configuration);
    }


    /// <summary>
    /// Creates metadata files for the database based on the provided configuration.
    /// </summary>
    /// <param name="configuration"></param>
    private void CreateDatabaseMetadata(DatabaseConfiguration configuration)
    {
        var normalizedName = NormalizeDatabaseName(configuration.DataConfig.DatabaseName, nameof(configuration.DataConfig.DatabaseName));

        var metadataRoot = Path.Combine(Directory.GetCurrentDirectory(), "Metadata");
        var metadataDirectory = Path.Combine(metadataRoot, normalizedName);
        Directory.CreateDirectory(metadataDirectory);
        Directory.CreateDirectory(Path.Combine(metadataDirectory, "Tables"));

        var databaseMetadataFileName = configuration.DataConfig.DatabaseName + "-Metadata.json";
        var databaseMetadataFilePath = Path.Combine(metadataDirectory, databaseMetadataFileName);
        var dbMetadata = new DatabaseMetadata
        {
            DatabaseName = configuration.DataConfig.DatabaseName,
            Owner = "Administrator",
            Description = "Metadata for database " + configuration.DataConfig.DatabaseName
        };
        using var stream = File.Create(databaseMetadataFilePath);
        JsonSerializer.Serialize(stream, dbMetadata, SerializerOptions);
    }

    /// <summary>
    /// Removes all tables from the specified database without dropping the database entry itself.
    /// </summary>
    public void ClearDatabase(string databaseName)
    {
        var normalizedName = NormalizeDatabaseName(databaseName, nameof(databaseName));
        ExecuteWithWriteLock(() =>
        {
            var db = RequireDatabaseNormalized(normalizedName);
            db.Tables.Clear();
        });
    }


    /// <summary>
    /// Returns a snapshot of the currently loaded databases.
    /// </summary>
    /// <remarks>
    /// The returned collection is a read-only view that cannot be used to mutate or dispose the internal store.
    /// Each snapshot reflects the engine state at the time of the call.
    /// </remarks>
    public IReadOnlyList<Database> Databases
        => ExecuteWithReadLock(() => _databases.Values.ToArray());

    /// <summary>
    /// Loads database definitions from the Metadata folder and registers them inside the engine.
    /// </summary>
    public MetadataLoadSummary LoadMetadataSchemas(string? metadataRoot = null)
    {
        var summary = new MetadataLoadSummary();
        var root = ResolveMetadataRoot(metadataRoot);

        if (!Directory.Exists(root))
            return summary;

        foreach (var databaseDirectory in Directory.EnumerateDirectories(root, "*", SearchOption.TopDirectoryOnly))
        {
            var folderName = Path.GetFileName(databaseDirectory);
            if (string.IsNullOrWhiteSpace(folderName))
                continue;

            try
            {
                var databaseName = ResolveDatabaseNameFromMetadata(databaseDirectory, folderName, summary);
                if (string.IsNullOrWhiteSpace(databaseName))
                    continue;

                var tables = ReadTablesFromMetadata(databaseName, databaseDirectory, summary);
                RegisterMetadataTables(databaseName, tables);
                summary.DatabasesLoaded++;
                summary.TablesLoaded += tables.Count;
            }
            catch (Exception exception)
            {
                summary.Errors.Add(new MetadataLoadError
                {
                    DatabaseName = folderName,
                    Resource = databaseDirectory,
                    Message = exception.Message
                });
            }
        }

        return summary;
    }


    /// <summary>
    /// Drops an entire database.
    /// </summary>
    public bool DropDatabase(string dbName)
    {
        var normalizedName = NormalizeDatabaseName(dbName, nameof(dbName));
        return ExecuteWithWriteLock(() =>
        {
            if (!_databases.Remove(normalizedName, out var database))
                return false;

            database.Dispose();
            _configurations.TryRemove(normalizedName, out _);
            return true;
        });
    }


    /// <summary>
    /// Registers a database configuration without altering the in-memory data structures.
    /// </summary>
    public void RegisterConfiguration(DatabaseConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        configuration.Validate();
        var normalizedName = NormalizeDatabaseName(configuration.DataConfig.DatabaseName, nameof(configuration.DataConfig.DatabaseName));
        _configurations[normalizedName] = configuration;
    }


    /// <summary>
    /// Exposes all registered database configurations.
    /// </summary>
    public IReadOnlyDictionary<string, DatabaseConfiguration> GetAllConfigurations()
        => _configurations.ToDictionary(entry => entry.Key, entry => entry.Value);


    /// <summary>
    /// Attempts to hydrate the configured database from the most recent dump file.
    /// </summary>
    public bool TryLoadLatestDump(DatabaseConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var latest = EnumerateDumpFiles(configuration).FirstOrDefault();
        if (latest is null)
            return false;

        LoadDatabase(configuration.DataConfig.DatabaseName, latest.FullName);
        return true;
    }


    /// <summary>
    /// Enumerates dump files ordered from newest to oldest for the given configuration.
    /// </summary>
    private IEnumerable<FileInfo> EnumerateDumpFiles(DatabaseConfiguration configuration)
    {
        if (configuration is null)
            throw new ArgumentNullException(nameof(configuration));

        var directory = configuration.DataConfig.DumpDirectory;
        if (!Directory.Exists(directory))
            return Enumerable.Empty<FileInfo>();

        var dirInfo = new DirectoryInfo(directory);
        var searchPattern = $"{configuration.DataConfig.DumpFilePrefix}_*.json.gz";

        return dirInfo
            .EnumerateFiles(searchPattern, SearchOption.TopDirectoryOnly)
            .OrderByDescending(file => file.LastWriteTimeUtc);
    }


    /// <summary>
    /// Loads a JSON file into memory as a new database.
    /// </summary>
    public void LoadDatabase(string databaseName, string filePath)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty.", nameof(databaseName));
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Database file not found: {filePath}");

        var normalizedName = NormalizeDatabaseName(databaseName, nameof(databaseName));
        var snapshot = ReadCompressedJson<Dictionary<string, TableData>>(filePath);

        ExecuteWithWriteLock(() =>
        {
            CreateDatabaseUnsafe(normalizedName);
            var database = RequireDatabaseNormalized(normalizedName);
            PopulateDatabase(database, snapshot);
        });
    }


    /// <summary>
    /// Checks if a database exists.
    /// </summary>
    public bool Exists(string dbName)
    {
        var normalizedName = NormalizeDatabaseName(dbName, nameof(dbName));
        return ExecuteWithReadLock(() => _databases.ContainsKey(normalizedName));
    }


    /// <summary>
    /// Creates a table inside the specified database.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbName"></param>
    /// <param name="name"></param>
    /// sample : var mfTable = db.CreateTable<MutualFund>("MutualFunds");
    /// sample : mfTable.Insert(new MutualFund { FundName = "Axis Bluechip", Units = 120.5m, NAV = 42.75m, PurchaseDate = DateTime.Today.AddDays(-30) });
    /// <returns></returns>
    public Table<T> CreateTable<T>(string dbName, string? name = null) where T : class, new()
    {
        var normalizedDbName = NormalizeDatabaseName(dbName, nameof(dbName));
        var tableName = NormalizeTableName(name ?? typeof(T).Name, nameof(name));

        return ExecuteWithWriteLock(() =>
        {
            var db = RequireDatabaseNormalized(normalizedDbName);
            if (db.Tables.Any(t => string.Equals(t.Name, tableName, StringComparison.Ordinal)))
                throw new InvalidOperationException($"Table '{tableName}' already exists.");

            var table = new Table<T>(tableName);
            db.Tables.Add(table);
            return table;
        });
    }

    /// <summary>
    /// Number of tables in a specific database.
    /// </summary>
    /// <param name="dbName"></param>
    /// <returns></returns>
    public int TablesCount(string dbName)
    {
        var normalizedName = NormalizeDatabaseName(dbName, nameof(dbName));
        return ExecuteWithReadLock(() =>
        {
            var database = RequireDatabaseNormalized(normalizedName);
            return database.Tables.Count;
        });
    }


    /// <summary>
    /// Retrieves a table instance, ensuring both database and table exist.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the database or table cannot be located.
    /// </exception>
    public Table<T> GetTable<T>(string dbName, string tableName) where T : class, new()
        => ExecuteWithReadLock(() => RequireTable<T>(dbName, tableName));


    /// <summary>
    /// Removes a table from the specified database.
    /// </summary>
    public bool DropTable(string dbName, string tableName)
    {
        var normalizedDbName = NormalizeDatabaseName(dbName, nameof(dbName));
        var normalizedTableName = NormalizeTableName(tableName, nameof(tableName));
        return ExecuteWithWriteLock(() =>
        {
            var db = RequireDatabaseNormalized(normalizedDbName);
            return db.Tables.RemoveAll(t => string.Equals(t.Name, normalizedTableName, StringComparison.Ordinal)) > 0;
        });
    }


    /// <summary>
    /// Retrieves the structure and content of a database.
    /// Returns all table names and their current in-memory data.
    /// </summary>
    public Database? GetDatabase(string name)
    {
        var normalizedName = NormalizeDatabaseName(name, nameof(name));
        return ExecuteWithReadLock(() => GetDatabaseUnsafe(normalizedName));
    }


    /// <summary>
    /// Returns the configuration associated with the database, when available.
    /// </summary>
    public bool TryGetConfiguration(string databaseName, out DatabaseConfiguration configuration)
    {
        var normalizedName = NormalizeDatabaseName(databaseName, nameof(databaseName));
        return _configurations.TryGetValue(normalizedName, out configuration!);
    }


    private Database RequireDatabase(string dbName, string paramName)
    {
        var normalizedName = NormalizeDatabaseName(dbName, paramName);
        return RequireDatabaseNormalized(normalizedName);
    }


    private Table<T> RequireTable<T>(string dbName, string tableName) where T : class, new()
    {
        var db = RequireDatabase(dbName, nameof(dbName));
        var normalizedTableName = NormalizeTableName(tableName, nameof(tableName));
        var table = db.Tables.FirstOrDefault(db => string.Equals(db.Name, normalizedTableName, StringComparison.Ordinal));
        return table as Table<T> ?? throw new InvalidOperationException($"Table '{normalizedTableName}' not found in database '{dbName}'.");
    }


    private static string NormalizeDatabaseName(string? dbName, string paramName)
    {
        if (string.IsNullOrWhiteSpace(dbName))
            throw new ArgumentException("Database name cannot be empty.", paramName);

        return dbName.Trim();
    }


    private static string NormalizeTableName(string? tableName, string paramName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be empty.", paramName);

        return tableName.Trim();
    }

    private void ExecuteWithReadLock(Action action)
    {
        _lock.EnterReadLock();
        try
        {
            action();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private T ExecuteWithReadLock<T>(Func<T> action)
    {
        _lock.EnterReadLock();
        try
        {
            return action();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private void ExecuteWithWriteLock(Action action)
    {
        _lock.EnterWriteLock();
        try
        {
            action();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private T ExecuteWithWriteLock<T>(Func<T> action)
    {
        _lock.EnterWriteLock();
        try
        {
            return action();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private Database? GetDatabaseUnsafe(string normalizedName)
        => _databases.TryGetValue(normalizedName, out var database) ? database : null;

    private Database RequireDatabaseNormalized(string normalizedName)
    {
        var database = GetDatabaseUnsafe(normalizedName);
        return database ?? throw new InvalidOperationException($"Database '{normalizedName}' not found.");
    }

    private Database CreateDatabaseUnsafe(string normalizedName)
    {
        var existing = GetDatabaseUnsafe(normalizedName);
        if (existing is not null)
            return existing;

        var database = new Database { Name = normalizedName };
        _databases[normalizedName] = database;
        return database;
    }


    // Shared JSON settings so tables round-trip with consistent casing and field inclusion.
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        WriteIndented = true,
        IncludeFields = true,
        PropertyNameCaseInsensitive = true
    };


    // Type resolution cache avoids repeatedly loading metadata via reflection when restoring tables.
    private readonly ConcurrentDictionary<string, Type> _rowTypeCache = new(StringComparer.Ordinal);


    /// <summary>
    /// Serializes a single database into a compressed JSON dump on disk.
    /// </summary>
    public void DumpDatabase(string databaseName, string filePath)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty.", nameof(databaseName));
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        var normalizedName = NormalizeDatabaseName(databaseName, nameof(databaseName));
        var snapshot = ExecuteWithReadLock(() =>
        {
            var database = RequireDatabaseNormalized(normalizedName);
            return SnapshotDatabase(database);
        });

        WriteCompressedJson(snapshot, filePath);
    }


    /// <summary>
    /// Serializes the configured database to a timestamped file and returns the path.
    /// </summary>
    public string DumpDatabase(DatabaseConfiguration configuration, DateTime? timestampUtc = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var timestamp = timestampUtc ?? DateTime.UtcNow;
        var filePath = configuration.BuildDumpPath(timestamp);
        DumpDatabase(configuration, filePath);
        return filePath;
    }


    /// <summary>
    /// Serializes the configured database to the provided path and enforces retention.
    /// </summary>
    public void DumpDatabase(DatabaseConfiguration configuration, string filePath)
    {
        if (configuration is null)
            throw new ArgumentNullException(nameof(configuration));
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        configuration.Validate();
        DumpDatabase(configuration.DataConfig.DatabaseName, filePath);
        TrimSnapshotHistory(configuration);
    }


    /// <summary>
    /// Removes snapshot files beyond the configured retention policy.
    /// </summary>
    public void TrimSnapshotHistory(DatabaseConfiguration configuration)
    {
        if (configuration is null)
            throw new ArgumentNullException(nameof(configuration));

        var files = EnumerateDumpFiles(configuration).ToList();
        var retention = Math.Max(0, configuration.DataConfig.MaxSnapshotHistory);
        if (retention == 0)
        {
            foreach (var file in files)
            {
                TryDeleteFile(file);
            }
            return;
        }

        if (files.Count <= retention)
            return;

        foreach (var file in files.Skip(retention))
        {
            TryDeleteFile(file);
        }
    }


    /// <summary>
    /// Serializes every in-memory database into a single compressed snapshot.
    /// </summary>
    public void SaveAllDatabases(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        var snapshot = ExecuteWithReadLock(SnapshotAllDatabases);
        WriteCompressedJson(snapshot, filePath);
    }


    /// <summary>
    /// Recreates every database previously persisted via <see cref="SaveAllDatabases"/>.
    /// </summary>
    /// <summary>
    /// Restores every database previously persisted via <see cref="SaveAllDatabases"/>.
    /// </summary>
    public void LoadAllDatabases(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"File not found: {filePath}");

        var payload = ReadCompressedJson<Dictionary<string, Dictionary<string, TableData>>>(filePath);

        ExecuteWithWriteLock(() =>
        {
            foreach (var (dbName, tables) in payload)
            {
                if (string.IsNullOrWhiteSpace(dbName))
                    continue;

                var normalizedName = NormalizeDatabaseName(dbName, nameof(dbName));
                CreateDatabaseUnsafe(normalizedName);
                var database = RequireDatabaseNormalized(normalizedName);

                PopulateDatabase(database, tables);
            }
        });
    }


    /// <summary>
    /// Serializes every in-memory database to a JSON payload without writing to disk.
    /// </summary>
    public string SerializeDatabases()
    {
        var snapshot = ExecuteWithReadLock(SnapshotAllDatabases);
        return JsonSerializer.Serialize(snapshot, _jsonOptions);
    }


    /// <summary>
    /// Replaces all in-memory databases with the content represented by the provided JSON payload.
    /// </summary>
    public void DeserializeDatabases(string serializedDatabases)
    {
        if (string.IsNullOrWhiteSpace(serializedDatabases))
            throw new ArgumentException("Serialized database payload cannot be empty.", nameof(serializedDatabases));

        var payload = JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, TableData>>>(
            serializedDatabases,
            _jsonOptions) ?? throw new InvalidOperationException("Failed to deserialize databases payload.");

        ExecuteWithWriteLock(() =>
        {
            ResetDatabases();

            foreach (var (dbName, tables) in payload)
            {
                if (string.IsNullOrWhiteSpace(dbName))
                    continue;

                var normalizedName = NormalizeDatabaseName(dbName, nameof(dbName));
                CreateDatabaseUnsafe(normalizedName);
                var database = RequireDatabaseNormalized(normalizedName);

                PopulateDatabase(database, tables);
            }
        });
    }


    /// <summary>
    /// Converts the supplied database into a serializable snapshot structure.
    /// </summary>
    private Dictionary<string, TableData> SnapshotDatabase(Database database)
    {
        var snapshot = new Dictionary<string, TableData>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in database.Tables)
        {
            if (table is null || ShouldSkipTableSnapshot(table))
                continue;

            snapshot[table.Name] = SnapshotTable(table.Name, table);
        }

        return snapshot;
    }


    /// <summary>
    /// Builds a serializable snapshot for every loaded database.
    /// </summary>
    private Dictionary<string, Dictionary<string, TableData>> SnapshotAllDatabases()
    {
        var snapshot = new Dictionary<string, Dictionary<string, TableData>>(StringComparer.OrdinalIgnoreCase);

        foreach (var database in _databases.Values)
        {
            if (string.IsNullOrWhiteSpace(database.Name))
                continue;

            snapshot[database.Name] = SnapshotDatabase(database);
        }

        return snapshot;
    }

    /// <summary>
    /// Serializes a single table into its metadata and row collection.
    /// </summary>
    private TableData SnapshotTable(string tableName, ITable table)
    {
        if (table is null)
            throw new ArgumentNullException(nameof(table));

        var rows = new List<JsonElement>();
        foreach (var row in table.GetRows())
        {
            var jsonElement = JsonSerializer.SerializeToElement(row, table.DataType, _jsonOptions);
            rows.Add(jsonElement);
        }

        var typeName = table.DataType.AssemblyQualifiedName;
        if (string.IsNullOrWhiteSpace(typeName))
            throw new InvalidOperationException($"Unable to resolve a stable type name for table '{tableName}'.");

        return new TableData
        {
            TypeName = typeName,
            Rows = rows
        };
    }

    /// <summary>
    /// Clears and repopulates a database using a serialized snapshot.
    /// </summary>
    private void PopulateDatabase(Database database, IReadOnlyDictionary<string, TableData> snapshot)
    {
        foreach (var (tableName, tableData) in snapshot)
        {
            if (ShouldSkipSnapshotTable(tableData))
                continue;

            database.Tables[tableName] = HydrateTable(tableName, tableData);
        }
    }

    /// <summary>
    /// Recreates a table instance and populates it from serialized row data.
    /// </summary>
    private ITable HydrateTable(string tableName, TableData tableData)
    {
        var rowType = ResolveRowType(tableData.TypeName);
        var tableGenericType = typeof(Table<>).MakeGenericType(rowType);
        var tableInstance = (ITable)Activator.CreateInstance(tableGenericType, tableName)!;

        foreach (var rowElement in tableData.Rows)
        {
            var row = JsonSerializer.Deserialize(rowElement.GetRawText(), rowType, _jsonOptions);
            if (row is not null)
                tableInstance.Add(row);
        }

        return tableInstance;
    }

    /// <summary>
    /// Resolves row types from their serialized names using a cache to avoid repeated reflection.
    /// </summary>
    private Type ResolveRowType(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            throw new InvalidOperationException("Serialized table is missing its row type metadata.");

        return _rowTypeCache.GetOrAdd(
            typeName,
            static resolvedTypeName => Type.GetType(resolvedTypeName)
                ?? throw new InvalidOperationException($"Unknown row type '{resolvedTypeName}'."));
    }

    /// <summary>
    /// Writes the supplied payload as compressed JSON to disk.
    /// </summary>
    private void WriteCompressedJson<T>(T payload, string filePath)
    {
        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        using var fileStream = File.Create(filePath);
        using var gzipStream = new GZipStream(fileStream, CompressionLevel.SmallestSize, leaveOpen: false);
        JsonSerializer.Serialize(gzipStream, payload, _jsonOptions);
    }

    /// <summary>
    /// Reads and deserializes compressed JSON from disk into the requested type.
    /// </summary>
    private T ReadCompressedJson<T>(string filePath)
    {
        using var fileStream = File.OpenRead(filePath);
        using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress, leaveOpen: false);
        var payload = JsonSerializer.Deserialize<T>(gzipStream, _jsonOptions);
        return payload ?? throw new InvalidOperationException($"Failed to deserialize payload into {typeof(T)}.");
    }

    /// <summary>
    /// Attempts to delete a snapshot file but swallows IO/security errors to avoid crashing cleanup loops.
    /// </summary>
    private void TryDeleteFile(FileInfo file)
    {
        try
        {
            file.Delete();
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private static string ResolveMetadataRoot(string? metadataRoot)
        => string.IsNullOrWhiteSpace(metadataRoot)
            ? Path.Combine(Directory.GetCurrentDirectory(), "Metadata")
            : metadataRoot;

    private string ResolveDatabaseNameFromMetadata(string databaseDirectory, string fallbackName, MetadataLoadSummary summary)
    {
        var metadataFile = Path.Combine(databaseDirectory, $"{fallbackName}-Metadata.json");
        if (!File.Exists(metadataFile))
            return fallbackName;

        try
        {
            using var stream = File.OpenRead(metadataFile);
            var metadata = JsonSerializer.Deserialize<DatabaseMetadata>(stream, SerializerOptions);
            if (!string.IsNullOrWhiteSpace(metadata?.DatabaseName))
                return metadata.DatabaseName.Trim();
        }
        catch (Exception exception) when (exception is IOException or JsonException)
        {
            summary.Errors.Add(new MetadataLoadError
            {
                DatabaseName = fallbackName,
                Resource = metadataFile,
                Message = exception.Message
            });
        }

        return fallbackName;
    }

    private List<ITable> ReadTablesFromMetadata(string databaseName, string databaseDirectory, MetadataLoadSummary summary)
    {
        var tablesDirectory = Path.Combine(databaseDirectory, "Tables");
        if (!Directory.Exists(tablesDirectory))
            return new List<ITable>();

        var tables = new List<ITable>();
        foreach (var file in Directory.EnumerateFiles(tablesDirectory, "*.json", SearchOption.TopDirectoryOnly))
        {
            try
            {
                using var stream = File.OpenRead(file);
                var document = JsonSerializer.Deserialize<TableMetadataDocument>(stream, SerializerOptions);
                if (document is null)
                    continue;

                var tableName = string.IsNullOrWhiteSpace(document.TableName)
                    ? Path.GetFileNameWithoutExtension(file)
                    : document.TableName.Trim();
                var fields = document.Fields ?? new List<FieldMetadataDocument>();
                if (string.IsNullOrWhiteSpace(tableName) || fields.Count == 0)
                    continue;

                var rowType = MetadataRowTypeBuilder.GetOrCreate(databaseName, tableName, fields);
                var tableGenericType = typeof(Table<>).MakeGenericType(rowType);
                if (Activator.CreateInstance(tableGenericType, tableName) is ITable tableInstance)
                    tables.Add(tableInstance);
            }
            catch (Exception exception) when (exception is IOException or JsonException)
            {
                summary.Errors.Add(new MetadataLoadError
                {
                    DatabaseName = databaseName,
                    Resource = file,
                    Message = exception.Message
                });
            }
        }

        return tables;
    }

    private void RegisterMetadataTables(string databaseName, IReadOnlyList<ITable> tables)
    {
        var normalizedName = NormalizeDatabaseName(databaseName, nameof(databaseName));
        ExecuteWithWriteLock(() =>
        {
            var database = CreateDatabaseUnsafe(normalizedName);
            if (tables is null || tables.Count == 0)
                return;

            foreach (var table in tables)
            {
                if (table is null)
                    continue;

                var index = database.Tables.FindIndex(existing =>
                    string.Equals(existing.Name, table.Name, StringComparison.OrdinalIgnoreCase));

                if (index >= 0)
                {
                    database.Tables[index] = table;
                }
                else
                {
                    database.Tables.Add(table);
                }
            }
        });
    }

    private void ResetDatabases()
    {
        foreach (var database in _databases.Values)
            database.Dispose();

        _databases.Clear();
    }

    public void Dispose()
    {
        ExecuteWithWriteLock(() =>
        {
            ResetDatabases();
            _configurations.Clear();
        });

        _lock.Dispose();
    }

    private static bool ShouldSkipTableSnapshot(ITable table)
    {
        if (table is null)
            return false;

        return IsMetadataRowType(table.DataType);
    }

    private static bool ShouldSkipSnapshotTable(TableData tableData)
    {
        if (tableData is null)
            return false;

        return IsMetadataRowType(tableData.TypeName);
    }

    private static bool IsMetadataRowType(Type? type)
    {
        if (type is null)
            return false;

        var assembly = type.Assembly;
        if (assembly is null || !assembly.IsDynamic)
            return false;

        var assemblyName = assembly.GetName().Name;
        return string.Equals(assemblyName, MetadataAssemblyName, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsMetadataRowType(string? typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return false;

        return typeName.StartsWith(MetadataNamespacePrefix, StringComparison.OrdinalIgnoreCase)
            && typeName.Contains(MetadataAssemblyName, StringComparison.OrdinalIgnoreCase);
    }
}
