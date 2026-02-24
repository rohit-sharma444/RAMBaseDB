namespace RAMBaseDB.Infrastructure.Services;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RAMBaseDB.Infrastructure.Configuration;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Workspace;

public interface IWorkspaceInsightService
{
    Task<WorkspaceOverview> CaptureAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Aggregates engine state and filesystem metadata to power dashboard views.
/// </summary>
public sealed class WorkspaceInsightService : IWorkspaceInsightService
{
    private readonly DatabaseEngine _databaseEngine;
    private readonly DatabaseConfiguration _databaseConfiguration;
    private readonly WorkspaceOptions _options;
    private readonly ILogger<WorkspaceInsightService> _logger;

    public WorkspaceInsightService(
        DatabaseEngine databaseEngine,
        DatabaseConfiguration databaseConfiguration,
        IOptions<WorkspaceOptions> options,
        ILogger<WorkspaceInsightService> logger)
    {
        _databaseEngine = databaseEngine ?? throw new ArgumentNullException(nameof(databaseEngine));
        _databaseConfiguration = databaseConfiguration ?? throw new ArgumentNullException(nameof(databaseConfiguration));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task<WorkspaceOverview> CaptureAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var dbSnapshot = _databaseEngine.Databases;
        var databaseSummaries = dbSnapshot
            .Select(db => new DatabaseTableSummary(db.Name, db.Tables.Count))
            .OrderByDescending(summary => summary.TableCount)
            .ThenBy(summary => summary.DatabaseName, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var configurationCount = CountFiles(_options.ConfigurationDirectory, "*-config.json");
        var userDocuments = CountFiles(_options.UsersDirectory, "*.json");
        var snapshotFiles = EnumerateSnapshotFiles().ToList();
        var snapshotCount = snapshotFiles.Count;
        var snapshotFootprint = snapshotFiles.Sum(file => file.Length);
        var latestSnapshot = snapshotFiles.Count == 0
            ? null
            : snapshotFiles
                .OrderByDescending(file => file.LastWriteTimeUtc)
                .Select(file => (DateTimeOffset?)file.LastWriteTimeUtc)
                .FirstOrDefault();

        var snapshotHistory = snapshotFiles
            .OrderByDescending(file => file.LastWriteTimeUtc)
            .Take(_options.InsightSnapshotDepth)
            .Select(file => new SnapshotSummary(file.Name, file.LastWriteTimeUtc, file.Length))
            .ToArray();

        var overview = new WorkspaceOverview(
            DefaultDatabase: ResolveDefaultDatabase(),
            DatabaseCount: dbSnapshot.Count,
            TableCount: dbSnapshot.Sum(db => db.Tables.Count),
            SavedConfigurationCount: configurationCount,
            SnapshotCount: snapshotCount,
            SnapshotFootprintBytes: snapshotFootprint,
            LatestSnapshotUtc: latestSnapshot,
            UserDocumentCount: userDocuments,
            Databases: databaseSummaries,
            Snapshots: snapshotHistory,
            SnapshotDirectory: _options.SnapshotDirectory,
            ConfigurationDirectory: _options.ConfigurationDirectory,
            UsersDirectory: _options.UsersDirectory,
            MetadataDirectory: _options.MetadataDirectory);

        return Task.FromResult(overview);
    }

    private string ResolveDefaultDatabase()
    {
        var configured = _databaseConfiguration.DataConfig.DatabaseName;
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        return string.IsNullOrWhiteSpace(_options.DefaultDatabaseName)
            ? "default"
            : _options.DefaultDatabaseName;
    }

    private static int CountFiles(string directory, string searchPattern)
    {
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
            return 0;

        try
        {
            return Directory.EnumerateFiles(directory, searchPattern, SearchOption.TopDirectoryOnly).Count();
        }
        catch (IOException)
        {
            return 0;
        }
        catch (UnauthorizedAccessException)
        {
            return 0;
        }
    }

    private IEnumerable<FileInfo> EnumerateSnapshotFiles()
    {
        var directory = _options.SnapshotDirectory;
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
            return Enumerable.Empty<FileInfo>();

        try
        {
            var info = new DirectoryInfo(directory);
            return info.EnumerateFiles("*.json*", SearchOption.TopDirectoryOnly);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning(exception, "Unable to enumerate snapshot files from {Directory}", directory);
            return Enumerable.Empty<FileInfo>();
        }
    }
}
