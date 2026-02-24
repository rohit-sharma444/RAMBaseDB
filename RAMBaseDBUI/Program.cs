using Microsoft.Extensions.Options;
using RAMBaseDB.Infrastructure.Configuration;
using RAMBaseDB.Application;
using RAMBaseDB.Infrastructure.Services;
using RAMBaseDB.Domain.Entities;
using RAMBaseDBUI.Components;

namespace RAMBaseDBUI;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.AddRazorComponents()
            .AddInteractiveServerComponents();

        builder.Services.AddOptions<WorkspaceOptions>()
            .Bind(builder.Configuration.GetSection("Workspace"))
            .PostConfigure(options => options.Normalize());

        builder.Services.AddSingleton(provider =>
        {
            var options = provider.GetRequiredService<IOptions<WorkspaceOptions>>().Value;
            options.EnsureDirectories();

            var configuration = new DatabaseConfiguration(options.DefaultDatabaseName, options.SnapshotDirectory);
            configuration.DataConfig.ConfigurationDirectory = options.ConfigurationDirectory;
            configuration.DataConfig.DumpDirectory = options.SnapshotDirectory;
            configuration.DataConfig.DumpFilePrefix = options.DumpFilePrefix;
            configuration.DataConfig.EnableAutomaticSnapshots = options.EnableAutomaticSnapshots;
            configuration.DataConfig.SnapshotInterval = options.SnapshotInterval;
            configuration.DataConfig.MaxSnapshotHistory = options.MaxSnapshotHistory;
            configuration.DataConfig.AutoRestoreLatestDump = options.AutoRestoreLatestDump;
            configuration.EnsureConfigurationDirectoryExists();
            configuration.EnsureDumpDirectoryExists();
            return configuration;
        });

        builder.Services.AddSingleton(provider =>
        {
            var options = provider.GetRequiredService<IOptions<WorkspaceOptions>>().Value;
            return new DatabaseEngine(new Database { Name = options.DefaultDatabaseName });
        });
        builder.Services.AddSingleton<IWorkspaceInsightService, WorkspaceInsightService>();
        builder.Services.AddSingleton<ITableMetadataStorage, FileSystemTableMetadataStorage>();
        builder.Services.AddHostedService<WorkspaceInitializationService>();

        builder.Services.AddHostedService(provider =>
        {
            var engine = provider.GetRequiredService<DatabaseEngine>();
            var logger = provider.GetRequiredService<ILogger<DatabaseEngineService>>();
            var options = provider.GetRequiredService<IOptions<WorkspaceOptions>>().Value;
            options.EnsureDirectories();

            return new DatabaseEngineService(
                databaseEngine: engine,
                logger: logger,
                dumpInterval: options.SnapshotInterval,
                snapshotDirectory: options.SnapshotDirectory,
                sqlDatabaseName: options.DefaultDatabaseName,
                configurationDirectory: options.ConfigurationDirectory);
        });

        var app = builder.Build();

        if (!app.Environment.IsDevelopment())
        {
            app.UseExceptionHandler("/Error");
        }

        app.UseStaticFiles();
        app.UseAntiforgery();

        app.MapRazorComponents<App>()
            .AddInteractiveServerRenderMode();

        app.Run();
    }
}
