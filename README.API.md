# RAMBaseDB Public API

This companion README lists the public types and members that ship from the core `RAMBaseDB` library. It mirrors the shape of the namespaces so you can quickly locate an entry point without sifting through internal helpers or UI-only components.

## RAMBaseDB.Application

### DatabaseEngine
- `DatabaseEngine()`
- `IReadOnlyList<Database> Databases { get; }`
- `void CreateDatabase(string dbName)`
- `void CreateDatabase(DatabaseConfiguration configuration)`
- `void ClearDatabase(string databaseName)`
- `MetadataLoadSummary LoadMetadataSchemas(string? metadataRoot = null)`
- `bool DropDatabase(string dbName)`
- `void RegisterConfiguration(DatabaseConfiguration configuration)`
- `IReadOnlyDictionary<string, DatabaseConfiguration> GetAllConfigurations()`
- `bool TryLoadLatestDump(DatabaseConfiguration configuration)`
- `void LoadDatabase(string databaseName, string filePath)`
- `bool Exists(string dbName)`
- `Table<T> CreateTable<T>(string dbName, string? name = null)`
- `int TablesCount(string dbName)`
- `Table<T> GetTable<T>(string dbName, string tableName)`
- `bool DropTable(string dbName, string tableName)`
- `Database? GetDatabase(string name)`
- `bool TryGetConfiguration(string databaseName, out DatabaseConfiguration configuration)`
- `void DumpDatabase(string databaseName, string filePath)`
- `string DumpDatabase(DatabaseConfiguration configuration, DateTime? timestampUtc = null)`
- `void DumpDatabase(DatabaseConfiguration configuration, string filePath)`
- `void TrimSnapshotHistory(DatabaseConfiguration configuration)`
- `void SaveAllDatabases(string filePath)`
- `void LoadAllDatabases(string filePath)`
- `string SerializeDatabases()`
- `void DeserializeDatabases(string serializedDatabases)`
- `void Dispose()`

### SqlParser
- `SqlParser()`
- `SqlParser(DatabaseEngine databaseManager, string? defaultDatabaseName = null)`
- `SqlExecutionResult Execute(string sql)`
- `SqlExecutionResult Execute(string sql, string? databaseName)`

### SqlExecutionResult
- `SqlExecutionResult()`
- `IReadOnlyList<object>? Rows { get; }`
- `int AffectedRows { get; }`
- `bool IsQuery { get; }`
- `SqlExecutionResult FromRows(IReadOnlyList<object> rows)`
- `SqlExecutionResult FromNonQuery(int affectedRows)`

### UserManager
- `UserManager(string id, string name, string password, string confirmPassword, string defaultDatabase, bool userMustChangePassword = false, bool isActive = true)`
- `User Create()`
- `bool HasPassword()`
- `bool HasConfirmPassword()`
- `bool IsPasswordsMatch()`
- `void Activate()`
- `void Deactivate()`
- `void RequirePasswordChange()`
- `void ClearPasswordChangeRequirement()`
- `void Rename(string name)`
- `void SetDefaultDatabase(string defaultDatabase)`
- `void ChangePassword(string password, string confirmPassword, bool markAsNeedingChange = false)`
- `bool TryValidate(out IReadOnlyList<string> errors)`
- `void EnsureValid()`
- `override string ToString()`
- `Dictionary<string, object> ToDictionary()`
- `void SaveUser(string filePath)`
- `static User LoadUser(string filePath)`
- `IUser FromDictionary(IReadOnlyDictionary<string, object?> values)`
- `UserManager FromUser(User user)`
- `IUser Clone(Action<IUser>? mutate = null)`
- `override bool Equals(object? obj)`
- `bool Equals(User? other)`
- `override int GetHashCode()`
- `void Dispose()`

## RAMBaseDB.Domain

### Abstractions

**IUser**
- `bool HasPassword()`
- `bool IsPasswordsMatch()`
- `void Activate()`
- `void Deactivate()`
- `void RequirePasswordChange()`
- `void ClearPasswordChangeRequirement()`
- `void SetDefaultDatabase(string defaultDatabase)`
- `void ChangePassword(string password, string confirmPassword, bool markAsNeedingChange = false)`

**ITable**
- `string Name { get; set; }`
- `void Clear()`
- `void Add(object item)`
- `int Delete(Func<object, bool>? predicate)`
- `int Update(Func<object, bool>? predicate, Action<object> mutator)`
- `Type DataType { get; }`
- `IEnumerable GetRows()`

### Entities

**Table<T>**
- `Table()`
- `Table(string? name)`
- `string Name { get; set; }`
- `Type DataType { get; }`
- `void Clear()`
- `void Insert(T source)`
- `void InsertRange(IEnumerable<T> items)`
- `IQueryable<T> AsQueryable()`
- `IEnumerable<T> Where(Func<T, bool> predicate)`
- `List<T> ToList()`
- `T? FindByPrimaryKey(object key)`
- `bool DeleteByPrimaryKey(object key)`
- `int Delete(Func<T, bool> predicate)`
- `int Update(Func<T, bool> predicate, Action<T> mutator)`

**Tables**
- `void Add(ITable table)`
- `void Insert(int index, ITable table)`
- `void AddRange(IEnumerable<ITable> tables)`
- `void InsertRange(int index, IEnumerable<ITable> tables)`
- `ITable this[int index] { get; set; }`
- `ITable this[string tableName] { get; set; }`
- `void Dispose()`

**Database**
- `Database()`
- `string Name { get; set; }`
- `Tables Tables { get; set; }`
- `void Dispose()`

**Databases**
- `void Dispose()`

**User**
- `string Id { get; set; }`
- `string Name { get; set; }`
- `string Password { get; set; }`
- `string ConfirmPassword { get; set; }`
- `string DefaultDatabase { get; set; }`
- `bool UserMustChangePassword { get; set; }`
- `bool IsActive { get; set; }`

**Users**
- `Users()`
- `Users(IEnumerable<KeyValuePair<int, User>> collection)`

### Configuration

**DatabaseConfigurationModel**
- `string DatabaseName`
- `bool DatabaseExists`
- `string ConfigurationDirectory`
- `string ConfigurationFile`
- `string DumpDirectory`
- `string DumpFilePrefix`
- `bool EnableAutomaticSnapshots`
- `TimeSpan SnapshotInterval`
- `int MaxSnapshotHistory`
- `bool AutoRestoreLatestDump`

### Schema

**TableStructure<T>**
- `TableStructure()`
- `TableStructure(string? tableName = null)`
- `Type EntityType { get; }`
- `string TableName { get; }`
- `IReadOnlyList<ColumnInfo> Columns { get; }`
- `ColumnInfo? PrimaryKey { get; }`
- `IReadOnlyList<ColumnInfo> RequiredColumns { get; }`
- `IReadOnlyList<ColumnInfo> ForeignKeyColumns { get; }`

**ColumnInfo**
- `ColumnInfo(PropertyInfo prop)`
- `PropertyInfo Property { get; }`
- `string Name { get; }`
- `bool IsPrimaryKey { get; }`
- `bool IsRequired { get; }`
- `bool IsAutoIncrement { get; }`
- `bool IsForeignKey { get; }`
- `Type? ForeignKeyReferencedType { get; }`
- `PrimaryKeyAttribute`
- `RequiredAttribute`
- `AutoIncrementAttribute`
- `ForeignKeyAttribute(Type referencedType)`

### Metadata

**DatabaseMetadata**
- `string DatabaseName { get; set; }`
- `DateTime CreatedAt { get; }`
- `DateTime LastModifiedAt { get; }`
- `string Owner { get; set; }`
- `string Description { get; set; }`

**TableMetadataDocument**
- `string DatabaseName { get; set; }`
- `string TableName { get; set; }`
- `List<FieldMetadataDocument> Fields { get; set; }`

**FieldMetadataDocument**
- `string Name { get; set; }`
- `string DataType { get; set; }`
- `int Length { get; set; }`
- `bool AllowBlank { get; set; }`
- `bool AutoGenerated { get; set; }`

**MetadataLoadSummary**
- `int DatabasesLoaded { get; internal set; }`
- `int TablesLoaded { get; internal set; }`
- `List<MetadataLoadError> Errors { get; }`
- `bool HasErrors { get; }`

**MetadataLoadError**
- `string? DatabaseName { get; init; }`
- `string? Resource { get; init; }`
- `string Message { get; init; }`

### Value objects

**TableData**
- `string TypeName { get; set; }`
- `List<JsonElement> Rows { get; set; }`

### Workspace

**WorkspaceOverview**
- `string DefaultDatabase`
- `int DatabaseCount`
- `int TableCount`
- `int SavedConfigurationCount`
- `int SnapshotCount`
- `long SnapshotFootprintBytes`
- `DateTimeOffset? LatestSnapshotUtc`
- `int UserDocumentCount`
- `IReadOnlyList<DatabaseTableSummary> Databases`
- `IReadOnlyList<SnapshotSummary> Snapshots`
- `string SnapshotDirectory`
- `string ConfigurationDirectory`
- `string UsersDirectory`
- `string MetadataDirectory`

**DatabaseTableSummary**
- `string DatabaseName`
- `int TableCount`

**SnapshotSummary**
- `string FileName`
- `DateTimeOffset LastWriteTimeUtc`
- `long SizeBytes`

## RAMBaseDB.Infrastructure

### Configuration

**DatabaseConfiguration**
- `DatabaseConfigurationModel DataConfig { get; set; }`
- `DatabaseConfiguration()`
- `DatabaseConfiguration(string databaseName, string dumpDirectory)`
- `Task<DatabaseConfigurationModel> LoadAsync(CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<string>> ListConfigurationsAsync(CancellationToken cancellationToken = default)`
- `Task SaveAsync(DatabaseConfigurationModel configuration, CancellationToken cancellationToken = default)`
- `string BuildDumpPath(DateTime timestampUtc)`
- `Task DeleteAsync(string? databaseName, CancellationToken cancellationToken = default)`
- `void Validate()`
- `void EnsureDumpDirectoryExists()`
- `void EnsureConfigurationDirectoryExists()`

**WorkspaceOptions**
- `string DefaultDatabaseName { get; set; }`
- `string SnapshotDirectory { get; set; }`
- `string ConfigurationDirectory { get; set; }`
- `string MetadataDirectory { get; set; }`
- `string UsersDirectory { get; set; }`
- `string DumpFilePrefix { get; set; }`
- `TimeSpan SnapshotInterval { get; set; }`
- `int MaxSnapshotHistory { get; set; }`
- `bool EnableAutomaticSnapshots { get; set; }`
- `bool AutoRestoreLatestDump { get; set; }`
- `bool BootstrapMetadata { get; set; }`
- `int InsightSnapshotDepth { get; set; }`
- `void Normalize()`
- `void EnsureDirectories()`

### Services

**DatabaseEngineService**
- `DatabaseEngineService(DatabaseEngine? databaseEngine = null, SqlParser? sqlParser = null, ILogger<DatabaseEngineService>? logger = null, TimeSpan? dumpInterval = null, string? snapshotDirectory = null, string? sqlDatabaseName = null, string? configurationDirectory = null)`
- `Task StartAsync(CancellationToken cancellationToken)`
- `Task<SqlExecutionResult> ExecuteSqlAsync(string sql, string? databaseName = null, CancellationToken cancellationToken = default)`
- `IReadOnlyList<Database> GetLoadedDatabases()`

**SqlParserService**
- `SqlParserService(DatabaseEngine? databaseManager = null, ILogger<SqlParserService>? logger = null, string? databaseName = null)`
- `Task<SqlExecutionResult> EnqueueAsync(string sql, CancellationToken cancellationToken = default)`
- `Task StopAsync(CancellationToken cancellationToken)`
- `void Dispose()`

**WorkspaceInitializationService**
- `WorkspaceInitializationService(DatabaseEngine databaseEngine, IOptions<WorkspaceOptions> options, ILogger<WorkspaceInitializationService> logger)`
- `Task StartAsync(CancellationToken cancellationToken)`
- `Task StopAsync(CancellationToken cancellationToken)`

**IWorkspaceInsightService / WorkspaceInsightService**
- `Task<WorkspaceOverview> CaptureAsync(CancellationToken cancellationToken = default)`
