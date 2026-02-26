using StagePipe.Web.Services;
using StagePipe.Web.Services.Connectivity;
using StagePipe.Web.Services.Sync;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorPages();
builder.Services.AddSingleton<ISshTunnelManager, SshTunnelManager>();
builder.Services.AddSingleton<IDatabaseBrowserService, DatabaseBrowserService>();
builder.Services.AddSingleton<ISyncTask, CourtCaseStatisticsStatusesToStatusTask>();
builder.Services.AddSingleton<ISyncTaskService, SyncTaskService>();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
	app.UseExceptionHandler("/Error");
	app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();

app.MapRazorPages();

app.Run();
