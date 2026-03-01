namespace StagePipe.Web.Services.Sync;

public interface ISqlScriptProvider
{
    string GetScript(string relativePath);
}
