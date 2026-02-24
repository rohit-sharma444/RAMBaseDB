namespace RAMBaseDB.Infrastructure.Configuration;

using RAMBaseDB.Domain.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

/// <summary>
/// Provides configuration and utility methods for managing database dump and snapshot settings, including directory
/// handling and validation.
/// </summary>
/// <remarks>This class encapsulates logic for normalizing and validating database configuration parameters,
/// ensuring that required directories exist and that snapshot settings are valid. It is designed to be used as a
/// central point for database-related configuration tasks, such as constructing dump file paths and enforcing
/// configuration constraints. Thread safety is not guaranteed; callers should ensure appropriate synchronization if
/// accessed concurrently.</remarks>

public sealed class DatabaseConfiguration
{
    private static readonly HashSet<char> InvalidFileNameCharacters = new(Path.GetInvalidFileNameChars());

    public DatabaseConfigurationModel DataConfig;

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    public DatabaseConfiguration()
        => DataConfig = new DatabaseConfigurationModel();

    public DatabaseConfiguration(string databaseName, string dumpDirectory)
    {
        DataConfig = new DatabaseConfigurationModel()
        {
            DatabaseName = NormalizeDatabaseName(databaseName),
            DumpDirectory = NormalizeDumpDirectory(dumpDirectory)
        };
    }

    public async Task<DatabaseConfigurationModel> LoadAsync(CancellationToken cancellationToken = default)
    {
        var configurationFilePath = ResolveConfigurationFilePath();
        if (configurationFilePath is null)
            return new DatabaseConfigurationModel();

        await using var stream = File.OpenRead(configurationFilePath);
        var model = await JsonSerializer.DeserializeAsync<DatabaseConfigurationModel>(
            stream,
            SerializerOptions,
            cancellationToken);

        DataConfig = model ?? new DatabaseConfigurationModel();
        return DataConfig;
    }

    public Task<IReadOnlyList<string>> ListConfigurationsAsync(CancellationToken cancellationToken = default)
    {
        var configurationDirectory = DataConfig.ConfigurationDirectory;

        if (!Directory.Exists(configurationDirectory))
            return Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

        var configurations = new List<string>();

        foreach (var file in Directory.EnumerateFiles(configurationDirectory, "*-config.json", SearchOption.TopDirectoryOnly))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var name = Path.GetFileNameWithoutExtension(file);
            if (!string.IsNullOrWhiteSpace(name))
                configurations.Add(name);
        }

        configurations.Sort(StringComparer.OrdinalIgnoreCase);
        return Task.FromResult<IReadOnlyList<string>>(configurations);
    }

    private string? ResolveConfigurationFilePath()
    {
        var configurationDirectory = DataConfig.ConfigurationDirectory;

        if (string.IsNullOrWhiteSpace(configurationDirectory))
            return null;

        if (!Directory.Exists(configurationDirectory))
            return null;

        if (!string.IsNullOrWhiteSpace(DataConfig.DatabaseName))
        {
            var targetedPath = Path.Combine(configurationDirectory, DataConfig.ConfigurationFile);
            if (File.Exists(targetedPath))
                return targetedPath;
        }

        foreach (var file in Directory.EnumerateFiles(configurationDirectory, "*-config.json", SearchOption.TopDirectoryOnly))
            return file;

        return null;
    }

    /// <summary>
    /// Asynchronously saves the specified database configuration to a JSON file in the configured directory.
    /// </summary>
    /// <remarks>If the configuration directory does not exist, it will be created. The configuration is
    /// serialized to a file named 'config.json' within the specified directory.</remarks>
    /// <param name="configuration">The database configuration model to be saved. Cannot be null. The configuration's directory must be accessible
    /// and writable.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the save operation.</param>
    /// <returns>A task that represents the asynchronous save operation.</returns>
    public async Task SaveAsync(DatabaseConfigurationModel configuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // Ensure the directory exists
        Directory.CreateDirectory(configuration.ConfigurationDirectory);

        // Create file path inside the directory
        var filePath = Path.Combine(configuration.ConfigurationDirectory, configuration.ConfigurationFile);

        await using var stream = File.Create(filePath);

        // Ensure the directory exists
        Directory.CreateDirectory(configuration.DumpDirectory);

        await JsonSerializer.SerializeAsync(stream, configuration, SerializerOptions, cancellationToken);
    }

    /// <summary>
    /// Ensures the dump directory exists and returns an absolute path for a timestamped file.
    /// </summary>
    public string BuildDumpPath(DateTime timestampUtc)
    {
        EnsureDumpDirectoryExists();
        var fileName = $"{DataConfig.DumpFilePrefix}_{timestampUtc:yyyyMMdd_HHmmss}.json.gz";
        return Path.Combine(DataConfig.DumpDirectory, fileName);
    }

    /// <summary>
    /// Removes the saved configuration file for the specified database if it exists.
    /// </summary>
    public async Task DeleteAsync(string? databaseName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty.", nameof(databaseName));

        var normalizedName = NormalizeDatabaseName(databaseName);
        var configurationDirectory = DataConfig.ConfigurationDirectory;

        if (!Directory.Exists(configurationDirectory))
            return;

        var configurationFile = Path.Combine(configurationDirectory, $"{normalizedName}-config.json");

        if (!File.Exists(configurationFile))
            return;

        cancellationToken.ThrowIfCancellationRequested();

        DatabaseConfigurationModel? storedConfiguration;
        using (var stream = File.OpenRead(configurationFile))
        {
            storedConfiguration = await JsonSerializer.DeserializeAsync<DatabaseConfigurationModel>(
                stream,
                SerializerOptions,
                cancellationToken);
        }

        if (storedConfiguration?.DatabaseExists == true)
            throw new InvalidOperationException($"Database '{normalizedName}' has already been created and its configuration cannot be deleted.");

        cancellationToken.ThrowIfCancellationRequested();
        File.Delete(configurationFile);
    }

    /// <summary>
    /// Validates optional properties beyond the constructor guarantees.
    /// </summary>
    public void Validate()
    {
        if (DataConfig.EnableAutomaticSnapshots && DataConfig.SnapshotInterval <= TimeSpan.Zero)
            throw new InvalidOperationException("Snapshot interval must be greater than zero.");
        if (DataConfig.MaxSnapshotHistory <= 0)
            throw new InvalidOperationException("Snapshot history must be greater than zero.");
    }

    /// <summary>
    /// Creates the dump directory if it does not exist.
    /// </summary>
    public void EnsureDumpDirectoryExists()
        => Directory.CreateDirectory(DataConfig.DumpDirectory);

    public void EnsureConfigurationDirectoryExists()
    => Directory.CreateDirectory(DataConfig.ConfigurationDirectory);

    private static string NormalizeDatabaseName(string? databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty.", nameof(databaseName));

        return databaseName.Trim();
    }

    private static string NormalizeDumpDirectory(string? dumpDirectory)
    {
        if (string.IsNullOrWhiteSpace(dumpDirectory))
            throw new ArgumentException("Dump directory cannot be empty.", nameof(dumpDirectory));

        var expanded = Environment.ExpandEnvironmentVariables(dumpDirectory.Trim());
        return Path.GetFullPath(expanded);
    }

    private static string NormalizeDumpFilePrefix(string? prefix)
    {
        if (string.IsNullOrWhiteSpace(prefix))
            throw new ArgumentException("Dump file prefix cannot be empty.", nameof(DataConfig.DumpFilePrefix));

        var trimmed = prefix.Trim();
        var buffer = trimmed.ToCharArray();

        for (var i = 0; i < buffer.Length; i++)
        {
            if (InvalidFileNameCharacters.Contains(buffer[i]) || char.IsWhiteSpace(buffer[i]))
                buffer[i] = '_';
        }

        var sanitized = new string(buffer);

        if (string.IsNullOrWhiteSpace(sanitized))
            throw new ArgumentException("Dump file prefix must contain at least one valid character.", nameof(DataConfig.DumpFilePrefix));

        return sanitized;
    }

    private static TimeSpan ValidateSnapshotInterval(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(DataConfig.SnapshotInterval), "Snapshot interval must be greater than zero.");

        return interval;
    }

    private static int ValidateSnapshotHistory(int history)
    {
        if (history <= 0)
            throw new ArgumentOutOfRangeException(nameof(DataConfig.MaxSnapshotHistory), "Snapshot history must be greater than zero.");

        return history;
    }
}
