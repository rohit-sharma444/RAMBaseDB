namespace RAMBaseDBUI.Services;

using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using RAMBaseDBUI.Models;

public interface IDatabaseConfigurationStore
{
    Task<DatabaseConfigurationModel> LoadAsync(CancellationToken cancellationToken = default);
    Task SaveAsync(DatabaseConfigurationModel configuration, CancellationToken cancellationToken = default);
}

internal sealed class FileDatabaseConfigurationStore : IDatabaseConfigurationStore
{
    private readonly string _configurationFilePath;
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    public FileDatabaseConfigurationStore(IWebHostEnvironment environment)
    {
        ArgumentNullException.ThrowIfNull(environment);

        var dataDirectory = Path.Combine(environment.ContentRootPath, "App_Data");
        Directory.CreateDirectory(dataDirectory);
        _configurationFilePath = Path.Combine(dataDirectory, "database-configuration.json");
    }

    public async Task<DatabaseConfigurationModel> LoadAsync(CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_configurationFilePath))
            return new DatabaseConfigurationModel();

        await using var stream = File.OpenRead(_configurationFilePath);
        var model = await JsonSerializer.DeserializeAsync<DatabaseConfigurationModel>(
            stream,
            SerializerOptions,
            cancellationToken);

        return model ?? new DatabaseConfigurationModel();
    }

    public async Task SaveAsync(DatabaseConfigurationModel configuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        await using var stream = File.Create(_configurationFilePath);
        await JsonSerializer.SerializeAsync(stream, configuration, SerializerOptions, cancellationToken);
    }
}
