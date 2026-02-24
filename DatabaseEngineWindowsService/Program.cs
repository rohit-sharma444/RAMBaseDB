using DatabaseEngineWindowsService;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using RAMBaseDB.Application;
using RAMBaseDB.Infrastructure.Services;
using RAMBaseDB.Domain.Entities;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseWindowsService(options =>
{
    options.ServiceName = builder.Configuration["Service:Name"] ?? "RAMBaseDB Database Engine";
});

builder.Services.Configure<DatabaseEngineWindowsServiceOptions>(
    builder.Configuration.GetSection(DatabaseEngineWindowsServiceOptions.SectionName));

builder.Services.AddSingleton(provider =>
{
    var options = provider.GetRequiredService<IOptions<DatabaseEngineWindowsServiceOptions>>().Value;
    var sqlDatabaseName = string.IsNullOrWhiteSpace(options.SqlDatabaseName)
        ? "default"
        : options.SqlDatabaseName.Trim();
    var engine = new DatabaseEngine(new Database { Name = sqlDatabaseName });

    if (!engine.Exists(sqlDatabaseName))
        engine.CreateDatabase(sqlDatabaseName);

    return engine;
});

builder.Services.AddSingleton(provider =>
{
    var options = provider.GetRequiredService<IOptions<DatabaseEngineWindowsServiceOptions>>().Value;
    var engine = provider.GetRequiredService<DatabaseEngine>();
    return new SqlParser(engine, options.SqlDatabaseName);
});

builder.Services.AddSingleton(provider =>
{
    var options = provider.GetRequiredService<IOptions<DatabaseEngineWindowsServiceOptions>>().Value;
    var logger = provider.GetRequiredService<ILogger<DatabaseEngineService>>();
    var engine = provider.GetRequiredService<DatabaseEngine>();
    var parser = provider.GetRequiredService<SqlParser>();

    return new DatabaseEngineService(
        databaseEngine: engine,
        sqlParser: parser,
        logger: logger,
        dumpInterval: options.ResolveDumpInterval(),
        snapshotDirectory: options.ResolveSnapshotDirectory(),
        sqlDatabaseName: options.SqlDatabaseName,
        configurationDirectory: options.ResolveConfigurationDirectory());
});

builder.Services.AddHostedService(sp => sp.GetRequiredService<DatabaseEngineService>());

var app = builder.Build();

app.MapGet("/api/health", (DatabaseEngine engine) =>
{
    var response = new
    {
        Status = "Healthy",
        Databases = engine.Databases.Count,
        TimestampUtc = DateTimeOffset.UtcNow
    };

    return Results.Ok(response);
});

app.MapGet("/api/databases", (DatabaseEngine engine) =>
{
    var payload = engine.Databases
        .Select(DatabaseResponseMapper.From)
        .OrderBy(db => db.Name, StringComparer.OrdinalIgnoreCase)
        .ToArray();

    return Results.Ok(payload);
});

app.MapPost("/api/sql", async Task<IResult> (
    SqlExecuteRequest request,
    DatabaseEngineService service,
    CancellationToken cancellationToken) =>
{
    if (request is null || string.IsNullOrWhiteSpace(request.Sql))
    {
        return Results.Problem(
            detail: "SQL statement is required.",
            statusCode: StatusCodes.Status400BadRequest);
    }

    try
    {
        var result = await service.ExecuteSqlAsync(request.Sql, request.Database, cancellationToken)
            .ConfigureAwait(false);
        return Results.Ok(SqlResponseMapper.FromResult(result));
    }
    catch (ArgumentException ex)
    {
        return Results.Problem(ex.Message, statusCode: StatusCodes.Status400BadRequest);
    }
    catch (InvalidOperationException ex)
    {
        return Results.Problem(ex.Message, statusCode: StatusCodes.Status409Conflict);
    }
    catch (Exception ex)
    {
        return Results.Problem(
            detail: $"Failed to execute SQL. {ex.Message}",
            statusCode: StatusCodes.Status500InternalServerError);
    }
})
.WithName("ExecuteSql");

app.Run();
