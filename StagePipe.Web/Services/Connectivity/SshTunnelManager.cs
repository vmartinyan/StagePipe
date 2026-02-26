using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StagePipe.Web.Options;

namespace StagePipe.Web.Services.Connectivity;

public sealed class SshTunnelManager : ISshTunnelManager, IDisposable
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<SshTunnelManager> _logger;
    private readonly Dictionary<string, Process> _ownedProcesses = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public SshTunnelManager(IConfiguration configuration, ILogger<SshTunnelManager> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public async Task EnsureForDatabaseAsync(string databaseName, CancellationToken cancellationToken)
    {
        var isProductionFamily = string.Equals(databaseName, "Production", StringComparison.OrdinalIgnoreCase)
            || string.Equals(databaseName, "ProductionAuth", StringComparison.OrdinalIgnoreCase);

        var sectionName = isProductionFamily
            ? "SshProduction"
            : "SshStaging";

        var tunnelOptions = _configuration.GetSection(sectionName).Get<SshTunnelOptions>() ?? new SshTunnelOptions();

        if (!tunnelOptions.Enabled || tunnelOptions.LocalPort <= 0)
        {
            return;
        }

        if (await IsPortOpenAsync("127.0.0.1", tunnelOptions.LocalPort, cancellationToken))
        {
            return;
        }

        if (!tunnelOptions.AutoStart)
        {
            throw new InvalidOperationException($"SSH tunnel for {databaseName} is not open on 127.0.0.1:{tunnelOptions.LocalPort}.");
        }

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            if (await IsPortOpenAsync("127.0.0.1", tunnelOptions.LocalPort, cancellationToken))
            {
                return;
            }

            if (_ownedProcesses.TryGetValue(sectionName, out var existing) && !existing.HasExited)
            {
                await WaitForPortAsync(tunnelOptions.LocalPort, cancellationToken);
                return;
            }

            ValidateOptions(sectionName, tunnelOptions);

            var args = BuildSshArgs(tunnelOptions);
            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "ssh",
                    Arguments = args,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    RedirectStandardError = true,
                    RedirectStandardOutput = true
                },
                EnableRaisingEvents = true
            };

            process.Start();
            _ownedProcesses[sectionName] = process;

            await WaitForPortAsync(tunnelOptions.LocalPort, cancellationToken);

            _logger.LogInformation("SSH tunnel ready for {Database} on 127.0.0.1:{Port}", databaseName, tunnelOptions.LocalPort);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private static string BuildSshArgs(SshTunnelOptions options)
    {
        return string.Join(' ',
            "-N",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-L", $"{options.LocalPort}:{options.RemoteHost}:{options.RemotePort}",
            "-p", options.Port,
            "-i", Quote(options.KeyPath),
            $"{options.User}@{options.Host}");
    }

    private static string Quote(string value) => $"\"{value}\"";

    private static void ValidateOptions(string sectionName, SshTunnelOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.Host) || string.IsNullOrWhiteSpace(options.User) || string.IsNullOrWhiteSpace(options.KeyPath) || string.IsNullOrWhiteSpace(options.RemoteHost))
        {
            throw new InvalidOperationException($"{sectionName} is missing required SSH fields.");
        }
    }

    private static async Task<bool> IsPortOpenAsync(string host, int port, CancellationToken cancellationToken)
    {
        using var client = new TcpClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(800));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);

        try
        {
            await client.ConnectAsync(host, port, linked.Token);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static async Task WaitForPortAsync(int port, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(8);
        while (DateTime.UtcNow < deadline)
        {
            if (await IsPortOpenAsync("127.0.0.1", port, cancellationToken))
            {
                return;
            }

            await Task.Delay(250, cancellationToken);
        }

        throw new InvalidOperationException($"Failed to open SSH local tunnel on 127.0.0.1:{port}.");
    }

    public void Dispose()
    {
        foreach (var process in _ownedProcesses.Values)
        {
            try
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                }
            }
            catch
            {
            }
            finally
            {
                process.Dispose();
            }
        }

        _ownedProcesses.Clear();
        _semaphore.Dispose();
    }
}
