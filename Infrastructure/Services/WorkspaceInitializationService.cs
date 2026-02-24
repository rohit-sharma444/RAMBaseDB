namespace RAMBaseDB.Infrastructure.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RAMBaseDB.Infrastructure.Configuration;
using RAMBaseDB.Application;

/// <summary>
/// Ensures workspace directories exist and metadata is loaded before handling requests.
/// </summary>
public sealed class WorkspaceInitializationService : IHostedService
{
    private readonly DatabaseEngine _databaseEngine;
    private readonly WorkspaceOptions _options;
    private readonly ILogger<WorkspaceInitializationService> _logger;

    public WorkspaceInitializationService(
        DatabaseEngine databaseEngine,
        IOptions<WorkspaceOptions> options,
        ILogger<WorkspaceInitializationService> logger)
    {
        _databaseEngine = databaseEngine ?? throw new ArgumentNullException(nameof(databaseEngine));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _options.Normalize();
        _options.EnsureDirectories();

        EnsureDefaultDatabase();
        BootstrapMetadata();

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
        => Task.CompletedTask;

    private void EnsureDefaultDatabase()
    {
        try
        {
            if (!_databaseEngine.Exists(_options.DefaultDatabaseName))
            {
                _databaseEngine.CreateDatabase(_options.DefaultDatabaseName);
                _logger.LogInformation("Created default database '{DatabaseName}'.", _options.DefaultDatabaseName);
            }
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Failed to ensure default database '{DatabaseName}'.", _options.DefaultDatabaseName);
        }
    }

    private void BootstrapMetadata()
    {
        if (!_options.BootstrapMetadata)
            return;

        try
        {
            var summary = _databaseEngine.LoadMetadataSchemas(_options.MetadataDirectory);
            if (summary.DatabasesLoaded > 0)
            {
                _logger.LogInformation(
                    "Loaded {Databases} metadata database(s) containing {Tables} table(s).",
                    summary.DatabasesLoaded,
                    summary.TablesLoaded);
            }

            if (summary.HasErrors)
            {
                foreach (var error in summary.Errors)
                {
                    _logger.LogWarning(
                        "Metadata error for {Database} ({Resource}): {Message}",
                        error.DatabaseName ?? "unknown",
                        error.Resource ?? "unknown",
                        error.Message);
                }
            }
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Failed to bootstrap workspace metadata from {Directory}.", _options.MetadataDirectory);
        }
    }
}
