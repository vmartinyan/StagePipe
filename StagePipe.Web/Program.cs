using StagePipe.Web.Services;
using StagePipe.Web.Services.Connectivity;
using StagePipe.Web.Services.Sync;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorPages();
builder.Services.AddMemoryCache();
builder.Services.AddSingleton<ISshTunnelManager, SshTunnelManager>();
builder.Services.AddSingleton<IDatabaseBrowserService, DatabaseBrowserService>();
builder.Services.AddSingleton<ISqlScriptProvider, SqlScriptProvider>();
builder.Services.AddSingleton<ISyncTask, StatusInitialInsert>();
builder.Services.AddSingleton<ISyncTask, JudgeInitialInsert>();
builder.Services.AddSingleton<ISyncTask, CourtCaseInitialInsert>();
builder.Services.AddSingleton<ISyncTask, ClaimInitialInsert>();
builder.Services.AddSingleton<ISyncTask, CitizenInitialInsert>();
builder.Services.AddSingleton<ISyncTask, CorporationInitialInsert>();
builder.Services.AddSingleton<ISyncTask, ParticipantTypeInitialInsert>();
builder.Services.AddSingleton<ISyncTask, ParticipantInitialInsert>();
builder.Services.AddSingleton<ISyncTask, ClaimRequirementsInitialInsert>();
builder.Services.AddSingleton<ISyncTask, AnswerInitialInsert>();
builder.Services.AddSingleton<ISyncTask, CourtCaseJudgeInitialInsert>();
builder.Services.AddSingleton<ISyncTask, JudicialActInitialInsert>();
builder.Services.AddSingleton<ISyncTask, JudicialActHistoryInitialInsert>();
builder.Services.AddSingleton<ISyncTask, JudicialActSubTypeInitialInsert>();
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
