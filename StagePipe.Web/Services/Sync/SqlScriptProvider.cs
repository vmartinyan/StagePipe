using Microsoft.Extensions.Caching.Memory;

namespace StagePipe.Web.Services.Sync;

public sealed class SqlScriptProvider : ISqlScriptProvider
{
    private readonly IWebHostEnvironment _environment;
    private readonly IMemoryCache _cache;

    public SqlScriptProvider(IWebHostEnvironment environment, IMemoryCache cache)
    {
        _environment = environment;
        _cache = cache;
    }

    public string GetScript(string relativePath)
    {
        var normalized = relativePath.Replace('\\', '/').TrimStart('/');
        return _cache.GetOrCreate($"sql:{normalized}", entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(30);

            var fullPath = Path.Combine(_environment.ContentRootPath, "Sql", normalized.Replace('/', Path.DirectorySeparatorChar));
            if (!File.Exists(fullPath))
            {
                throw new FileNotFoundException($"SQL script file was not found: {fullPath}");
            }

            return File.ReadAllText(fullPath);
        }) ?? throw new InvalidOperationException($"Failed to load SQL script: {normalized}");
    }
}
