# RAMBaseDB

RAMBaseDB is a .NET 8 library that implements a typed, in-memory relational database kernel with optional persistence, a lightweight SQL surface, and background services for automation. It is designed for scenarios that need relational behavior (tables, constraints, SQL semantics) without the overhead of a full RDBMS.

## Feature highlights
- **Typed relational core**: `DatabaseEngine` hosts any number of named databases while `Table<T>` enforces schema-first constraints, primary/foreign keys, auto-increment values, and validation with reader/writer locking.
- **SQL pipeline**: `SqlParser` and `SqlEngineService` interpret `SELECT`, `INSERT`, `UPDATE`, and `DELETE` with joins, GROUP BY, aggregates, ORDER BY, and background queueing so dynamic workloads never block callers.
- **Persistence controls**: `DatabaseSerializer` plus `DatabaseConfiguration` output compressed `.json.gz` dumps, auto-restore the most recent state, and trim snapshot history to keep disk usage predictable.
- **Background automation**: Hosted services (`SqlEngineService`, `DatabaseSnapshotService`) plug into `Microsoft.Extensions.Hosting`, enabling SQL queues, health logging, and scheduled dumps inside worker processes.
- **Persistent workspace cache**: `PersistentWorkspace<TKey, TValue>` provides TTL-based caching, parent/child invalidation, and JSON persistence to the `Data/` folder for memoizing expensive reports.
- **Sample payroll UI**: `SampleApplication/PayrollRazorApp` showcases how to seed data, execute SQL, and surface payroll screens entirely on top of RAMBaseDB.
- **Guard rails**: `RAMBaseDB.Tests` (xUnit + coverlet) covers the engine, SQL parser, hosted services, and locking semantics to prevent regressions.

## Repository layout

| Path | Purpose |
| --- | --- |
| `Domain/` | Business entities (`Database`, `Table<T>`), schemas, metadata documents, abstractions, and value objects. |
| `Application/` | Use-case services such as `DatabaseEngine`, `SqlParser`, `UserManager`, and dynamic metadata builders. |
| `Infrastructure/Configuration/` | File-system-aware helpers (`DatabaseConfiguration`, `WorkspaceOptions`) for dumps + workspace settings. |
| `Infrastructure/Services/` | Hosted services for SQL execution queues, background snapshots, and workspace insight/bootstrap flows. |
| `SampleApplication/` | Razor Pages payroll sample (companies, departments, employees, attendance, and employee records) backed by RAMBaseDB. |
| `RAMBaseDB.Tests/` | xUnit test project targeting `net8.0`. |
| `RAMBaseDB.sln` | Solution file that ties projects and tests together. |

## Prerequisites
- [.NET SDK 8.0](https://dotnet.microsoft.com/download) (required for build/test).
- PowerShell 7+ or a bash-compatible shell for running CLI commands.
- Optional: Visual Studio 2022, Rider, or VS Code for development.

## Build steps
1. Restore NuGet packages for every project: `dotnet restore RAMBaseDB.sln`.
2. Compile the library, tests, and sample apps: `dotnet build RAMBaseDB.sln -c Debug` (swap to `-c Release` for optimized output).
3. Run the full solution test suite before committing: `dotnet test RAMBaseDB.sln /p:CollectCoverage=true`.
4. Optional sanity check—launch the Razor payroll sample: `dotnet run --project SampleApplication/PayrollRazorApp/PayrollRazorApp.csproj`.

## Quick start

### 1. Define an entity (schema-first)
```csharp
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Schema;

public class Product
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    [Required]
    public string Name { get; set; } = string.Empty;

    public string? Sku { get; set; }
    public decimal Price { get; set; }
}
```

### 2. Create a database and interact with a table
```csharp
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;

var manager = new DatabaseEngine(new Database { Name = "Inventory" });
manager.CreateDatabase("Inventory");

var products = manager.CreateTable<Product>("Inventory", "Products");

products.Insert(new Product { Name = "Widget", Sku = "W-001", Price = 42.75m });
products.InsertRange(new[]
{
    new Product { Name = "Flux Capacitor", Sku = "FC-001", Price = 199.99m },
    new Product { Name = "Sprocket", Sku = "SP-010", Price = 12.50m }
});

var updated = products.Update(p => p.Sku == "SP-010", p => p.Price = 15m);
var premium = products.AsQueryable()
                      .Where(p => p.Price > 40)
                      .ToList();

var deleted = products.Delete(p => p.Price < 20);
```

## Usage notes
- Register `DatabaseEngine` as a singleton in your DI container (or create it once per process) so reader/writer locks and table metadata stay consistent across workers.
- Define tables up front with `CreateTable<T>` and keep references to the resulting `Table<T>` instances; bulk inserts/updates are fastest when you reuse those handles instead of re-querying by name.
- Reach for `SqlParser` or `SqlEngineService` when you need dynamic SQL, multi-tenant scripting, or queued work, and fall back to typed table APIs for critical hot paths.
- Configure persistence with `DatabaseConfiguration`, placing dumps outside `bin/` or `obj/`, and pair it with `DatabaseSnapshotService` when you need automated snapshots or auto-restore on boot.
- Use `PersistentWorkspace<TKey, TValue>` to cache expensive SQL results, link related cache keys, and let the built-in TTL cleanup keep disk usage stable.

## Executing SQL

`SqlParser` converts SQL text into in-memory operations via `System.Linq.Dynamic.Core`. Supply an existing `DatabaseEngine` and, optionally, the default database name.

```csharp
using RAMBaseDB.Application;

var parser = new SqlParser(manager, defaultDatabaseName: "Inventory");

parser.Execute("INSERT INTO Products (Name, Sku, Price) VALUES ('Adapter', 'AD-001', 9.99)");

var report = parser.Execute(@"
    SELECT Name, Price
    FROM Products
    WHERE Price >= 40
    ORDER BY Price DESC
");

if (report.IsQuery && report.Rows is { } rows)
{
    foreach (dynamic row in rows)
    {
        Console.WriteLine($"{row.Name}: {row.Price}");
    }
}
```

`SqlExecutionResult` returns `Rows`, `AffectedRows`, and `IsQuery`, making it easy to inspect query output or non-query effects. For asynchronous, multi-producer workloads, enqueue SQL through `SqlEngineService`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Infrastructure.Services;

builder.Services.AddSingleton(sp => new DatabaseEngine(new Database { Name = "Inventory" }));
builder.Services.AddSingleton<SqlEngineService>(sp =>
    new SqlEngineService(
        sp.GetRequiredService<DatabaseEngine>(),
        sp.GetRequiredService<ILogger<SqlEngineService>>(),
        databaseName: "Inventory"));
builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<SqlEngineService>());

var host = builder.Build();
var engine = host.Services.GetRequiredService<SqlEngineService>();
await engine.EnqueueAsync("DELETE FROM Products WHERE Price < 5");
```

## Persistence and snapshots

`DatabaseConfiguration` centralizes dump settings (directories, prefixes, snapshot cadence, retention). `DatabaseEngine` exposes overloads for dumping, loading, and trimming history.

```csharp
using RAMBaseDB.Infrastructure.Configuration;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;

var dumpRoot = Path.Combine(AppContext.BaseDirectory, "dumps");

var configuration = new DatabaseConfiguration("Inventory", dumpRoot)
{
    DataConfig = new DatabaseConfigurationModel
    {
        DatabaseName = "Inventory",
        DumpDirectory = dumpRoot,
        ConfigurationDirectory = dumpRoot,
        ConfigurationFile = Path.Combine(dumpRoot, "inventory.json"),
        DumpFilePrefix = "inventory",
        EnableAutomaticSnapshots = true,
        SnapshotInterval = TimeSpan.FromMinutes(5),
        MaxSnapshotHistory = 10,
        AutoRestoreLatestDump = true
    }
};

var dumpPath = manager.DumpDatabase(configuration); // returns the generated file path
manager.TrimSnapshotHistory(configuration);          // enforce retention
manager.LoadDatabase("Inventory", dumpPath);        // hydrate from disk
```

To automate snapshots, schedule `DatabaseSnapshotService` (internal to the library) inside your worker process:

```csharp
using RAMBaseDB.Infrastructure.Services;

builder.Services.AddSingleton<IHostedService>(sp =>
    new DatabaseSnapshotService(configuration, runImmediately: true));
```

This service writes dumps on the configured interval and cooperates with `DatabaseEngine`.

## Persistent workspace cache

`PersistentWorkspace<TKey, TValue>` stores cached values on disk and provides parent/child linking for cascading invalidations.

```csharp
using RAMBaseDB;

PersistentWorkspace<string, SqlExecutionResult>.SetDefaultExpiration(TimeSpan.FromMinutes(10));

var key = "report:inventory:premium";
PersistentWorkspace<string, SqlExecutionResult>.Set(key, report);

var cached = PersistentWorkspace<string, SqlExecutionResult>.Get(key);
var exists = PersistentWorkspace<string, SqlExecutionResult>.Contains(key);

PersistentWorkspace<string, SqlExecutionResult>.Link("report:inventory", key);
PersistentWorkspace<string, SqlExecutionResult>.Remove(key);
```

Data is serialized to `Data/workspace.json`, and a background timer cleans up expired entries once per minute.

## Payroll sample application

`SampleApplication/PayrollRazorApp` is a Razor Pages UI that provisions a RAMBaseDB database named `Payroll` and offers:
- Company, department, and employee master screens with validation built on top of the in-memory tables.
- Attendance capture with status + check-in/out tracking that enforces foreign keys against employees.
- Employee record entry (bonuses, salary adjustments, etc.) to illustrate how payroll events can be linked to people.

Run it locally with:

```pwsh
dotnet run --project SampleApplication/PayrollRazorApp/PayrollRazorApp.csproj
```

The app seeds a few companies, departments, employees, attendance entries, and employee records so every screen lights up immediately. All changes stay in memory for the lifetime of the web host, which keeps the sample self-contained.

## Public API documentation

Looking for the full list of public types and members? See `README.PublicFunctions.md` for a namespace-by-namespace catalog of everything exposed by `RAMBaseDB.Domain`, `RAMBaseDB.Application`, and `RAMBaseDB.Infrastructure`.

## Testing

All behavioral coverage lives in `RAMBaseDB.Tests`:

```pwsh
dotnet test RAMBaseDB.Tests/RAMBaseDB.Tests.csproj
```

The test project references `Microsoft.NET.Test.Sdk`, `xunit`, `xunit.runner.visualstudio`, and `coverlet.collector`. To gather coverage:

```pwsh
dotnet test RAMBaseDB.Tests/RAMBaseDB.Tests.csproj -c Release /p:CollectCoverage=true
```

Beyond the existing `DatabaseEngine`, `Table`, and `SqlParser` suites, `SqlParserServiceTests` now exercises the hosted SQL queue end-to-end (successful background execution, cancellation handling, and invalid-input guards) to protect the worker-friendly surface.

Lock-heavy areas are also covered by concurrency tests such as `SerializeDatabases_AllowsConcurrentTableCreation` and `ConcurrentReadersAndWriter_RemainThreadSafe`, ensuring the shared/exclusive locking stays intact.

## Next steps

- Extend the SQL grammar or add new table constraints inside `Core/SqlParser.cs` and `Models/Table.cs`.
- Wire hosted services into an ASP.NET Core worker to execute SQL scripts or roll snapshots automatically.
- Contribute additional documentation (examples, diagrams) or tests by submitting pull requests.
