namespace RAMBaseDB.Infrastructure.Services;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RAMBaseDB.Infrastructure.Configuration;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Configuration;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Metadata;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Background worker that keeps the in-memory databases hydrated and periodically persists them to disk.
/// </summary>
/// <remarks>The service bootstraps the <see cref="DatabaseEngine"/> from the latest snapshot when available,
/// exposes a simple SQL execution helper backed by <see cref="SqlParser"/>, and writes both a plain JSON payload and a
/// compressed dump of every database on a fixed cadence. Use this service when RAMBaseDB should continuously mirror its
/// volatile state onto disk without blocking request threads.</remarks>
public sealed class DatabaseEngineService : BackgroundService
{
    private const string DefaultDatabaseName = "default";
    private readonly DatabaseEngine _databaseEngine;
    private readonly SqlParser _sqlParser;
    private readonly ILogger<DatabaseEngineService>? _logger;
    private readonly TimeSpan _dumpInterval;
    private readonly string _snapshotDirectory;
    private readonly string _serializedSnapshotPath;
    private readonly string _compressedSnapshotPath;
    private readonly string _sqlDatabaseName;
    private readonly string _configurationDirectory;
    private volatile bool _initialLoadComplete;
    private static readonly JsonSerializerOptions ConfigurationSerializerOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    public DatabaseEngineService(
        DatabaseEngine? databaseEngine = null,
        SqlParser? sqlParser = null,
        ILogger<DatabaseEngineService>? logger = null,
        TimeSpan? dumpInterval = null,
        string? snapshotDirectory = null,
        string? sqlDatabaseName = null,
        string? configurationDirectory = null)
    {
        _sqlDatabaseName = string.IsNullOrWhiteSpace(sqlDatabaseName)
            ? DefaultDatabaseName
            : sqlDatabaseName.Trim();

        _databaseEngine = databaseEngine ?? new DatabaseEngine(new Database { Name = _sqlDatabaseName });

        if (!_databaseEngine.Exists(_sqlDatabaseName))
            _databaseEngine.CreateDatabase(_sqlDatabaseName);

        _sqlParser = sqlParser ?? new SqlParser(_databaseEngine, _sqlDatabaseName);
        _logger = logger;

        _dumpInterval = dumpInterval ?? TimeSpan.FromSeconds(900);
        if (_dumpInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(dumpInterval), "Dump interval must be greater than zero.");

        _snapshotDirectory = string.IsNullOrWhiteSpace(snapshotDirectory)
            ? Path.Combine(Directory.GetCurrentDirectory(), "Snapshots")
            : snapshotDirectory;

        _serializedSnapshotPath = Path.Combine(_snapshotDirectory, "databases.json");
        _compressedSnapshotPath = Path.Combine(_snapshotDirectory, "databases.json.gz");
        _configurationDirectory = string.IsNullOrWhiteSpace(configurationDirectory)
            ? Path.Combine(Directory.GetCurrentDirectory(), "Config")
            : ResolveAbsolutePath(configurationDirectory);
    }

    /// <inheritdoc />
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await LoadDatabasesIntoMemoryAsync(cancellationToken).ConfigureAwait(false);
        _initialLoadComplete = true;
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_initialLoadComplete)
        {
            await LoadDatabasesIntoMemoryAsync(stoppingToken).ConfigureAwait(false);
            _initialLoadComplete = true;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PersistSnapshotAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to persist database snapshot.");
            }

            try
            {
                await Task.Delay(_dumpInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Loads all known databases back into memory if a snapshot already exists.
    /// </summary>
    private async Task LoadDatabasesIntoMemoryAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_snapshotDirectory);

        var restored = false;

        if (File.Exists(_compressedSnapshotPath))
        {
            _logger?.LogInformation("Loading databases from compressed snapshot '{FilePath}'.", _compressedSnapshotPath);
            _databaseEngine.LoadAllDatabases(_compressedSnapshotPath);
            restored = true;
        }
        else if (File.Exists(_serializedSnapshotPath))
        {
            var payload = await File.ReadAllTextAsync(_serializedSnapshotPath, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(payload))
            {
                _logger?.LogWarning("Serialized snapshot '{FilePath}' was empty.", _serializedSnapshotPath);
            }
            else
            {
                _databaseEngine.DeserializeDatabases(payload);
                _logger?.LogInformation("Loaded databases from serialized snapshot '{FilePath}'.", _serializedSnapshotPath);
                restored = true;
            }
        }

        var registered = RegisterConfiguredDatabases(cancellationToken);
        if (registered)
            restored = true;

        if (TryRestoreConfiguredDatabases())
            restored = true;

        if (EnsureConfiguredDatabasesExist())
            restored = true;

        var metadataSummary = _databaseEngine.LoadMetadataSchemas();
        if (metadataSummary.DatabasesLoaded > 0)
        {
            _logger?.LogInformation(
                "Loaded {DatabaseCount} metadata database(s) containing {TableCount} table(s).",
                metadataSummary.DatabasesLoaded,
                metadataSummary.TablesLoaded);
            restored = true;
        }

        if (metadataSummary.HasErrors)
        {
            foreach (var error in metadataSummary.Errors)
            {
                _logger?.LogWarning(
                    "Metadata issue for {Database} at {Resource}: {Message}",
                    error.DatabaseName ?? "unknown",
                    error.Resource ?? "unknown",
                    error.Message);
            }
        }

        if (!restored)
        {
            var existing = _databaseEngine.Databases.Count;
            if (existing > 0)
                _logger?.LogInformation("Database engine already contains {Count} database(s) in memory.", existing);
        }
    }

    /// <summary>
    /// Serializes and dumps every in-memory database to disk.
    /// </summary>
    private async Task PersistSnapshotAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Directory.CreateDirectory(_snapshotDirectory);
        var serialized = _databaseEngine.SerializeDatabases();

        await File.WriteAllTextAsync(_serializedSnapshotPath, serialized, cancellationToken).ConfigureAwait(false);
        _databaseEngine.SaveAllDatabases(_compressedSnapshotPath);
        var dumpedCount = DumpLoadedDatabases();

        _logger?.LogInformation(
            "Serialized snapshot saved to '{JsonPath}' and '{CompressedPath}'. Dumped {Count} database(s).",
            _serializedSnapshotPath,
            _compressedSnapshotPath,
            dumpedCount);
    }

    /// <summary>
    /// Executes ad-hoc SQL statements against the managed database engine.
    /// </summary>
    public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, string? databaseName = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL statement cannot be empty.", nameof(sql));

        return Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _sqlParser.Execute(sql, databaseName ?? _sqlDatabaseName);
        }, cancellationToken);
    }

    /// <summary>
    /// Exposes the current in-memory databases as tracked by <see cref="DatabaseEngine"/>.
    /// </summary>
    public IReadOnlyList<Database> GetLoadedDatabases()
        => _databaseEngine.Databases;

    private bool RegisterConfiguredDatabases(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_configurationDirectory))
            return false;

        if (!Directory.Exists(_configurationDirectory))
        {
            _logger?.LogDebug("Configuration directory '{Directory}' does not exist; skipping configuration bootstrap.", _configurationDirectory);
            return false;
        }

        string[] files;
        try
        {
            files = Directory
                .EnumerateFiles(_configurationDirectory, "*-config.json", SearchOption.TopDirectoryOnly)
                .ToArray();
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            _logger?.LogWarning(exception, "Unable to enumerate database configurations from {Directory}.", _configurationDirectory);
            return false;
        }

        var registeredAny = false;

        foreach (var file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var configuration = TryLoadConfiguration(file);
            if (configuration is null || !configuration.DataConfig.DatabaseExists)
                continue;

            var databaseName = configuration.DataConfig.DatabaseName;
            if (string.IsNullOrWhiteSpace(databaseName))
                continue;

            try
            {
                _databaseEngine.RegisterConfiguration(configuration);
                registeredAny = true;
            }
            catch (Exception exception)
            {
                _logger?.LogError(exception, "Failed to register configuration for database '{Database}' from '{File}'.", databaseName, file);
            }
        }

        if (registeredAny)
        {
            _logger?.LogInformation("Registered database configurations from '{Directory}'.", _configurationDirectory);
        }

        return registeredAny;
    }

    private DatabaseConfiguration? TryLoadConfiguration(string filePath)
    {
        try
        {
            using var stream = File.OpenRead(filePath);
            var model = JsonSerializer.Deserialize<DatabaseConfigurationModel>(stream, ConfigurationSerializerOptions);
            if (model is null)
                return null;

            if (string.IsNullOrWhiteSpace(model.DatabaseName))
                model.DatabaseName = DeriveDatabaseNameFromFile(filePath);

            model.ConfigurationDirectory = _configurationDirectory;
            model.ConfigurationFile = Path.GetFileName(filePath);

            var configuration = new DatabaseConfiguration
            {
                DataConfig = model
            };

            return configuration;
        }
        catch (Exception exception) when (exception is IOException or JsonException or UnauthorizedAccessException)
        {
            _logger?.LogError(exception, "Failed to parse database configuration '{FilePath}'.", filePath);
            return null;
        }
    }

    private static string DeriveDatabaseNameFromFile(string filePath)
    {
        var fileName = Path.GetFileNameWithoutExtension(filePath);
        if (string.IsNullOrWhiteSpace(fileName))
            return string.Empty;

        const string suffix = "-config";
        return fileName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)
            ? fileName[..^suffix.Length]
            : fileName;
    }

    private bool EnsureConfiguredDatabasesExist()
    {
        var createdAny = false;

        foreach (var (name, configuration) in _databaseEngine.GetAllConfigurations())
        {
            if (string.IsNullOrWhiteSpace(name) || configuration is null)
                continue;

            if (_databaseEngine.Exists(name))
                continue;

            try
            {
                _databaseEngine.CreateDatabase(configuration);
                _logger?.LogInformation("Created database '{Database}' from configuration.", name);
                createdAny = true;
            }
            catch (Exception exception)
            {
                _logger?.LogError(exception, "Failed to create database '{Database}' from configuration.", name);
            }
        }

        return createdAny;
    }

    /// <summary>
    /// Restores each configured database from its latest retained dump, when available.
    /// </summary>
    private bool TryRestoreConfiguredDatabases()
    {
        var restoredAny = false;
        foreach (var (name, configuration) in _databaseEngine.GetAllConfigurations())
        {
            if (configuration is null)
                continue;

            try
            {
                if (_databaseEngine.TryLoadLatestDump(configuration))
                {
                    _logger?.LogInformation(
                        "Restored database '{Database}' from '{Directory}'.",
                        name,
                        configuration.DataConfig.DumpDirectory);
                    restoredAny = true;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to restore database '{Database}' from configured dumps.", name);
            }
        }

        return restoredAny;
    }

    /// <summary>
    /// Dumps every in-memory database to disk, respecting configuration when available.
    /// </summary>
    private int DumpLoadedDatabases()
    {
        var databases = _databaseEngine.Databases;
        var timestampUtc = DateTime.UtcNow;
        var dumped = 0;

        foreach (var database in databases)
        {
            if (database is null || string.IsNullOrWhiteSpace(database.Name))
                continue;

            var dumpPath = ResolveDumpPath(database.Name, timestampUtc);
            if (string.IsNullOrWhiteSpace(dumpPath))
                continue;

            try
            {
                _databaseEngine.DumpDatabase(database.Name, dumpPath);
                dumped++;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to dump database '{Database}'.", database.Name);
            }
        }

        return dumped;
    }

    /// <summary>
    /// Resolves a filesystem path for the supplied database dump, preferring configuration settings when present.
    /// </summary>
    private string? ResolveDumpPath(string databaseName, DateTime timestampUtc)
    {
        if (_databaseEngine.TryGetConfiguration(databaseName, out var configuration))
        {
            if (!configuration.DataConfig.EnableAutomaticSnapshots)
                return null;

            return configuration.BuildDumpPath(timestampUtc);
        }

        Directory.CreateDirectory(_snapshotDirectory);
        var fileName = $"{databaseName}_{timestampUtc:yyyyMMdd_HHmmss}.json.gz";
        return Path.Combine(_snapshotDirectory, fileName);
    }

    private static string ResolveAbsolutePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return Path.Combine(Directory.GetCurrentDirectory(), "Config");

        var expanded = Environment.ExpandEnvironmentVariables(path.Trim());
        return Path.GetFullPath(expanded);
    }
}
