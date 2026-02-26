namespace StagePipe.Web.Options;

public sealed class SshTunnelOptions
{
    public bool Enabled { get; set; } = true;
    public bool AutoStart { get; set; } = true;
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; } = 22;
    public string User { get; set; } = string.Empty;
    public string KeyPath { get; set; } = string.Empty;
    public int LocalPort { get; set; }
    public string RemoteHost { get; set; } = string.Empty;
    public int RemotePort { get; set; } = 3306;
}
