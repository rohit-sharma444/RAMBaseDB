namespace RAMBaseDB.Application;

using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

/// <summary>
/// Creates time-based backups for a single <see cref="DatabaseEngine"/> instance and restores the latest dump on demand.
/// </summary>
public sealed class DatabaseBackupManager : IDisposable
{
    private readonly DatabaseEngine _engine;
    private readonly string _databaseName;
    private readonly string _backupDirectory;
    private readonly TimeSpan _backupInterval;
    private readonly int _maxBackupHistory;
    private readonly ILogger? _logger;
    private readonly Timer _timer;
    private int _backupInProgress;
    private bool _disposed;

    public DatabaseBackupManager(
        DatabaseEngine engine,
        string databaseName,
        string? backupDirectory,
        TimeSpan? backupInterval = null,
        int maxBackupHistory = 12,
        ILogger? logger = null)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _databaseName = string.IsNullOrWhiteSpace(databaseName)
            ? throw new ArgumentException("Database name cannot be empty.", nameof(databaseName))
            : databaseName.Trim();

        _backupInterval = (backupInterval ?? TimeSpan.FromMinutes(5)) switch
        {
            var interval when interval > TimeSpan.Zero => interval,
            _ => throw new ArgumentOutOfRangeException(nameof(backupInterval), "Backup interval must be greater than zero.")
        };

        if (maxBackupHistory < 1)
            throw new ArgumentOutOfRangeException(nameof(maxBackupHistory), "Backup history must be at least 1.");

        _maxBackupHistory = maxBackupHistory;
        _logger = logger;
        _backupDirectory = ResolveBackupDirectory(backupDirectory, _databaseName);
        _timer = new Timer(OnTimerElapsed, null, _backupInterval, _backupInterval);
    }

    /// <summary>
    /// Attempts to hydrate the database from the newest retained backup.
    /// </summary>
    public bool TryRestoreLatestBackup()
    {
        ThrowIfDisposed();
        try
        {
            var latest = EnumerateBackups().FirstOrDefault();
            if (latest is null)
                return false;

            _engine.LoadDatabase(_databaseName, latest.FullName);
            _logger?.LogInformation(
                "Restored database '{Database}' from backup '{BackupFile}'.",
                _databaseName,
                latest.FullName);
            return true;
        }
        catch (Exception exception)
        {
            _logger?.LogWarning(
                exception,
                "Failed to restore database '{Database}' from backup.",
                _databaseName);
            return false;
        }
    }

    /// <summary>
    /// Immediately writes a backup, outside the normal interval cadence.
    /// </summary>
    public void TriggerBackup()
    {
        ThrowIfDisposed();
        RunBackup();
    }

    private void OnTimerElapsed(object? state)
    {
        if (_disposed)
            return;

        RunBackup();
    }

    private void RunBackup()
    {
        if (Interlocked.Exchange(ref _backupInProgress, 1) == 1)
            return;

        try
        {
            ExecuteBackup();
        }
        catch (Exception exception)
        {
            _logger?.LogError(exception, "Failed to create backup for database '{Database}'.", _databaseName);
        }
        finally
        {
            Interlocked.Exchange(ref _backupInProgress, 0);
        }
    }

    private void ExecuteBackup()
    {
        Directory.CreateDirectory(_backupDirectory);

        var filePath = BuildBackupPath(DateTime.UtcNow);
        _engine.DumpDatabase(_databaseName, filePath);
        TrimBackupHistory();

        _logger?.LogInformation(
            "Created backup '{BackupFile}' for database '{Database}'.",
            filePath,
            _databaseName);
    }

    private string BuildBackupPath(DateTime timestampUtc)
    {
        var fileName = $"{_databaseName}_{timestampUtc:yyyyMMdd_HHmmss}.json.gz";
        return Path.Combine(_backupDirectory, fileName);
    }

    private IEnumerable<FileInfo> EnumerateBackups()
    {
        var directory = new DirectoryInfo(_backupDirectory);
        if (!directory.Exists)
            return Enumerable.Empty<FileInfo>();

        return directory
            .EnumerateFiles($"{_databaseName}_*.json.gz", SearchOption.TopDirectoryOnly)
            .OrderByDescending(file => file.LastWriteTimeUtc);
    }

    private void TrimBackupHistory()
    {
        var staleBackups = EnumerateBackups()
            .Skip(_maxBackupHistory)
            .ToList();

        foreach (var file in staleBackups)
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
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _timer.Dispose();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DatabaseBackupManager));
    }

    private static string ResolveBackupDirectory(string? backupDirectory, string databaseName)
    {
        var root = string.IsNullOrWhiteSpace(backupDirectory)
            ? Path.Combine(Directory.GetCurrentDirectory(), "Snapshots", databaseName)
            : backupDirectory.Trim();

        var expanded = Environment.ExpandEnvironmentVariables(root);
        var absolute = Path.GetFullPath(expanded);
        Directory.CreateDirectory(absolute);
        return absolute;
    }
}
