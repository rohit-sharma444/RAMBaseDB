namespace RAMBaseDB.Infrastructure.Services;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;
using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

/// <summary>
/// Provides a background service for parsing and executing SQL statements asynchronously against a managed database
/// instance.
/// </summary>
/// <remarks>SqlParserService manages a queue of SQL commands, executing them in the background and returning
/// results via tasks. It ensures that the target database exists and is ready for command execution. The service is
/// designed for concurrent command submission, with a single background reader processing requests. Thread safety is
/// maintained for command queuing, but callers should avoid disposing the service while commands are pending. Logging
/// is supported via the provided ILogger instance. The service automatically cleans up resources and cancels pending
/// requests on shutdown.</remarks>
public sealed class SqlParserService : BackgroundService
{
    private const string DefaultDatabaseName = "default";
    private readonly Channel<SqlCommandRequest> _commandChannel;
    private readonly ILogger<SqlParserService>? _logger;
    private readonly SqlParser _sqlParser;
    private readonly DatabaseEngine _databaseManager;
    private readonly string _databaseName;

    public SqlParserService(DatabaseEngine? databaseManager = null, ILogger<SqlParserService>? logger = null, string? databaseName = null)
    {
        _logger = logger;
        _databaseName = string.IsNullOrWhiteSpace(databaseName) ? DefaultDatabaseName : databaseName.Trim();
        _databaseManager = databaseManager ?? new DatabaseEngine(new Database { Name = _databaseName });

        if (!_databaseManager.Exists(_databaseName))
            _databaseManager.CreateDatabase(_databaseName);

        _sqlParser = new SqlParser(_databaseManager, _databaseName);
        _commandChannel = Channel.CreateUnbounded<SqlCommandRequest>(new UnboundedChannelOptions
        {
            AllowSynchronousContinuations = false,
            SingleReader = true,
            SingleWriter = false
        });
    }

    /// <summary>
    /// Queues a SQL statement for background execution and returns a task that completes with the result.
    /// </summary>
    public Task<SqlExecutionResult> EnqueueAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL statement cannot be empty.", nameof(sql));

        var request = SqlCommandRequest.Create(sql, cancellationToken);

        if (!_commandChannel.Writer.TryWrite(request))
        {
            request.Dispose();
            throw new InvalidOperationException("SQL engine background service is not accepting new commands.");
        }

        return request.Completion.Task;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (await _commandChannel.Reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
            {
                while (_commandChannel.Reader.TryRead(out var request))
                {
                    using (request)
                    {
                        if (request.IsCanceled)
                        {
                            request.TrySetCanceled();
                            continue;
                        }

                        try
                        {
                            var result = _sqlParser.Execute(request.Sql, _databaseName);
                            request.TrySetResult(result);
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, "Failed to execute SQL command.");
                            request.TrySetException(ex);
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown flow.
        }
        finally
        {
            _commandChannel.Writer.TryComplete();

            while (_commandChannel.Reader.TryRead(out var pending))
            {
                using (pending)
                {
                    pending.TrySetCanceled();
                }
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _commandChannel.Writer.TryComplete();
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    public override void Dispose()
    {
        base.Dispose();
        _databaseManager.Dispose();
    }

    private sealed class SqlCommandRequest : IDisposable
    {
        private readonly CancellationTokenRegistration _registration;

        private SqlCommandRequest(string sql, CancellationToken cancellationToken)
        {
            Sql = sql;
            CancellationToken = cancellationToken;
            Completion = new TaskCompletionSource<SqlExecutionResult>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (cancellationToken.CanBeCanceled)
            {
                _registration = cancellationToken.Register(static state =>
                {
                    var self = (SqlCommandRequest)state!;
                    self.TrySetCanceled();
                }, this);
            }
        }

        public string Sql { get; }
        public CancellationToken CancellationToken { get; }
        public TaskCompletionSource<SqlExecutionResult> Completion { get; }
        public bool IsCanceled => CancellationToken.IsCancellationRequested;

        public static SqlCommandRequest Create(string sql, CancellationToken cancellationToken)
            => new(sql, cancellationToken);

        public bool TrySetResult(SqlExecutionResult result)
            => Completion.TrySetResult(result);

        public bool TrySetException(Exception exception)
            => Completion.TrySetException(exception);

        public bool TrySetCanceled()
        {
            if (CancellationToken.CanBeCanceled)
                return Completion.TrySetCanceled(CancellationToken);

            return Completion.TrySetCanceled();
        }

        public void Dispose()
            => _registration.Dispose();
    }
}
