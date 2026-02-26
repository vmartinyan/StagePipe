# StagePipe

Web-first .NET application for viewing MySQL tables and running sync services from a visual interface.

This repository is now **Web-only** (`StagePipe.Web`).

## What it does

- Connects to both databases (`ConnectionStrings:Production`, `ConnectionStrings:Staging`)
- Shows table data from Production or Staging
- Runs sync services from UI buttons

## Configure

Edit [StagePipe.Web/appsettings.json](StagePipe.Web/appsettings.json):

- `ConnectionStrings`
  - `Production`
  - `Staging`
- `SshProduction`
- `SshStaging`

## Run

From repository root:

```powershell
dotnet run --project .\StagePipe.Web\StagePipe.Web.csproj -- --urls http://127.0.0.1:5086
```

## Visual interface (table browser)

`StagePipe.Web` now uses Razor Pages architecture with separated layers:

- Pages/UI: `Pages/Index.cshtml`
- Separate pages: `Pages/Tables.cshtml`, `Pages/Sync.cshtml`
- Page logic: `Pages/*.cshtml.cs`
- Browse data service: `Services/DatabaseBrowserService.cs`
- Sync services: `Services/Sync/*`

Open `http://127.0.0.1:5086` and:

- go to **Show Tables** page to browse any table rows
- go to **Sync Services** page to execute sync tasks

Implemented service:

- `CourtCaseStatisticsStatuses -> status`
  - reads from Production table `CourtCaseStatisticsStatuses`
  - truncates Staging table `status`
  - inserts mapped fields: `Name -> name`, `tag -> slug`

To add more services for UI execution, add a new class in `StagePipe.Web/Services/Sync` that implements `ISyncTask`, then register it in `StagePipe.Web/Program.cs`.

## SSH tunnel for Production

Production DB is configured to connect through a local SSH tunnel on `127.0.0.1:3307`.

Open the tunnel before running sync:

```powershell
ssh -N -L 3307:192.168.133.34:3306 -p 22 -i "C:\Users\vahra\IUvpn" harmony@83.139.3.202
```

## SSH tunnel for Staging

Staging DB is configured to connect through a local SSH tunnel on `127.0.0.1:3308`.

Open the tunnel before running sync:

```powershell
ssh -N -L 3308:10.100.125.12:3306 -p 2224 -i "C:\Users\vahra\IUvpn" harmony@81.16.14.68
```

Then run the app in another terminal after both tunnels are open.

## Notes

- For secrets in real environments, prefer environment-specific config or secret storage over committing credentials.