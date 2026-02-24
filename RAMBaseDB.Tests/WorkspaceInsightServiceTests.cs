namespace RAMBaseDB.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Configuration;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Schema;
using RAMBaseDB.Infrastructure.Configuration;
using RAMBaseDB.Infrastructure.Services;
using Xunit;

public sealed class WorkspaceInsightServiceTests : IDisposable
{
    private readonly string _root;
    private readonly WorkspaceOptions _options;
    private readonly DatabaseEngine _engine;
    private readonly DatabaseConfiguration _configuration;

    public WorkspaceInsightServiceTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "RAMBaseDB.Tests", "Insight", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);

        _options = new WorkspaceOptions
        {
            SnapshotDirectory = Path.Combine(_root, "snapshots"),
            ConfigurationDirectory = Path.Combine(_root, "config"),
            MetadataDirectory = Path.Combine(_root, "metadata"),
            UsersDirectory = Path.Combine(_root, "users"),
            InsightSnapshotDepth = 3,
            DefaultDatabaseName = "Fallback"
        };
        _options.EnsureDirectories();

        _engine = new DatabaseEngine(new Database { Name = "Host" });
        _engine.CreateDatabase("Primary");
        var primaryItems = _engine.CreateTable<WorkspaceEntity>("Primary", "Items");
        primaryItems.Insert(new WorkspaceEntity { Name = "One", Email = "one@example.com" });

        _engine.CreateDatabase("Analytics");
        var events = _engine.CreateTable<WorkspaceEntity>("Analytics", "Events");
        events.Insert(new WorkspaceEntity { Name = "Event", Email = "event@example.com" });
        var sessions = _engine.CreateTable<WorkspaceEntity>("Analytics", "Sessions");
        sessions.Insert(new WorkspaceEntity { Name = "Session", Email = "session@example.com" });

        var dumpDirectory = Path.Combine(_root, "dumps");
        Directory.CreateDirectory(dumpDirectory);
        _configuration = new DatabaseConfiguration("Primary", dumpDirectory);
        _configuration.DataConfig.ConfigurationDirectory = _options.ConfigurationDirectory;
        _configuration.DataConfig.ConfigurationFile = "Primary-config.json";
    }

    [Fact]
    public async Task CaptureAsync_ReturnsAggregatedWorkspaceSnapshot()
    {
        var configurationFiles = new[]
        {
            CreateConfigurationFile("Primary-config.json"),
            CreateConfigurationFile("Analytics-config.json")
        };

        CreateUserDocument("admin");
        CreateUserDocument("auditor");

        var snapshots = CreateSnapshots(count: 4);

        var service = CreateService();
        var overview = await service.CaptureAsync();

        overview.DefaultDatabase.Should().Be("Primary");
        overview.DatabaseCount.Should().BeGreaterOrEqualTo(2);
        overview.TableCount.Should().Be(3);
        overview.SavedConfigurationCount.Should().Be(configurationFiles.Length);
        overview.UserDocumentCount.Should().Be(2);
        overview.SnapshotCount.Should().Be(4);
        overview.SnapshotFootprintBytes.Should().Be(snapshots.Sum(file => file.Length));
        overview.LatestSnapshotUtc.Should().BeCloseTo(
            snapshots.Max(file => file.LastWriteTimeUtc),
            TimeSpan.FromSeconds(1));

        overview.Databases.Should().NotBeEmpty();
        overview.Databases[0].DatabaseName.Should().Be("Analytics");
        overview.Databases[0].TableCount.Should().Be(2);
        overview.Databases.Should().Contain(summary => summary.DatabaseName == "Primary" && summary.TableCount == 1);

        overview.Snapshots.Should().HaveCount(_options.InsightSnapshotDepth);
        overview.Snapshots.Select(snapshot => snapshot.FileName).Should().ContainInOrder(
            "snapshot_000.json.gz",
            "snapshot_001.json.gz",
            "snapshot_002.json.gz");

        overview.SnapshotDirectory.Should().Be(_options.SnapshotDirectory);
        overview.ConfigurationDirectory.Should().Be(_options.ConfigurationDirectory);
        overview.UsersDirectory.Should().Be(_options.UsersDirectory);
        overview.MetadataDirectory.Should().Be(_options.MetadataDirectory);
    }

    [Fact]
    public async Task CaptureAsync_WhenDirectoriesAreMissing_ReturnsZeroedCounts()
    {
        DeleteDirectory(_options.ConfigurationDirectory);
        DeleteDirectory(_options.UsersDirectory);
        DeleteDirectory(_options.SnapshotDirectory);

        var service = CreateService();
        var overview = await service.CaptureAsync();

        overview.SavedConfigurationCount.Should().Be(0);
        overview.UserDocumentCount.Should().Be(0);
        overview.SnapshotCount.Should().Be(0);
        overview.SnapshotFootprintBytes.Should().Be(0);
        overview.Snapshots.Should().BeEmpty();
    }

    [Fact]
    public async Task CaptureAsync_WithCanceledToken_ThrowsOperationCanceled()
    {
        var service = CreateService();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => service.CaptureAsync(cancellationTokenSource.Token));
    }

    public void Dispose()
    {
        _engine.Dispose();
        DeleteDirectory(_root);
    }

    private WorkspaceInsightService CreateService()
        => new WorkspaceInsightService(
            _engine,
            _configuration,
            Options.Create(_options),
            NullLogger<WorkspaceInsightService>.Instance);

    private string CreateConfigurationFile(string name)
    {
        Directory.CreateDirectory(_options.ConfigurationDirectory);
        var path = Path.Combine(_options.ConfigurationDirectory, name);
        File.WriteAllText(path, "{}");
        return path;
    }

    private void CreateUserDocument(string userName)
    {
        Directory.CreateDirectory(_options.UsersDirectory);
        var path = Path.Combine(_options.UsersDirectory, $"{userName}.json");
        File.WriteAllText(path, $@"{{ ""user"": ""{userName}"" }}");
    }

    private IReadOnlyList<FileInfo> CreateSnapshots(int count)
    {
        Directory.CreateDirectory(_options.SnapshotDirectory);
        var result = new List<FileInfo>(count);
        var now = DateTimeOffset.UtcNow;

        for (var i = 0; i < count; i++)
        {
            var path = Path.Combine(_options.SnapshotDirectory, $"snapshot_{i:000}.json.gz");
            File.WriteAllText(path, $"payload-{i}");
            var timestamp = now.AddMinutes(-i);
            File.SetLastWriteTimeUtc(path, timestamp.UtcDateTime);
            result.Add(new FileInfo(path));
        }

        return result;
    }

    private static void DeleteDirectory(string? path)
    {
        if (string.IsNullOrWhiteSpace(path) || !Directory.Exists(path))
            return;

        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Ignore cleanup failures in tests.
        }
    }

    private sealed class WorkspaceEntity
    {
        [PrimaryKey]
        [AutoIncrement]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; } = string.Empty;

        public string Email { get; set; } = string.Empty;
    }
}
