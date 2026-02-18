<#
Distribute-Mailboxes.ps1

.SYNOPSIS
  Sammelt alle UserMailbox-Postfächer, ermittelt die Größe (MB) und verteilt sie gewichtet auf Ziel-Mailboxdatenbanken.
  Die Verteilung berücksichtigt zusätzlich den freien Speicher des Volumes, auf dem die jeweilige EDB liegt.

.DESCRIPTION
  - Holt alle UserMailbox-Mailboxen und ihre Größen (Get-MailboxStatistics).
  - Ermittelt je Mailbox-DB den freien Speicher (Win32_Volume) anhand des EDB-Pfads.
  - Filtert Ziel-DBs optional nach Mindest-Free%-Schwelle und Exclude/Include-Listen.
  - Greedy-Verteilung: große Mailboxen zuerst; pro Mailbox wird die DB mit dem kleinsten PROJIZIERTEN Score gewählt.

  Disk-aware Score (je DB, projiziert nach Zuweisung):
    baseUsedPenalty = (100 - FreePct) * 1000
    sizePressure    = (ProjectedSumMB / FreeMB) * 1_000_000
    countPressure   = ProjectedCount * 50
    Score           = baseUsedPenalty + sizePressure + countPressure

  Hinweis:
  - Mailboxen ohne verwertbare Statistics (z.B. nie angemeldet) bekommen SizeMB=0.
  - MoveRequests werden mit -SuspendWhenReadyToComplete erstellt.

.PARAMETER WhatIf
  Simulation: Es werden keine MoveRequests erstellt, nur die geplante Verteilung und WhatIf-Ausgaben.

.PARAMETER MinFreePercent
  Mailboxdatenbanken werden nur genutzt, wenn das zugehörige Volume mindestens diesen Free%-Wert hat.
  Standard: 25

.PARAMETER ExcludeDBs
  Exchange-Mailboxdatenbanken (NAME in Exchange) die ausgeschlossen werden sollen.

.PARAMETER IncludeDBs
  Wenn gesetzt, werden NUR diese Exchange-Mailboxdatenbanken berücksichtigt (nach Exclude/MinFreePercent).

.EXAMPLE
  Planung (Simulation):
    .\Distribute-Mailboxes.ps1 -WhatIf

  Nur DBs nutzen, deren Volume >= 50% frei hat:
    .\Distribute-Mailboxes.ps1 -WhatIf -MinFreePercent 50

  DB1 explizit ausschließen:
    .\Distribute-Mailboxes.ps1 -WhatIf -ExcludeDBs "DB01"

  Ausführung (MoveRequests erstellen):
    .\Distribute-Mailboxes.ps1

  Move-Status prüfen:
    Get-MoveRequest | Get-MoveRequestStatistics |
      Select DisplayName, Status, PercentComplete, TotalMailboxSize, TargetDatabase

  Finalisierung im Wartungsfenster:
    Get-MoveRequest -MoveStatus Suspended | Resume-MoveRequest
#>

[CmdletBinding()]
param(
    [switch]$WhatIf,

    [int]$MinFreePercent = 25,

    # Optional: zusätzliche Sicherheit für die Log-Projektion bei Moves.
    # Faustformel: MoveSumGB * LogGrowthFactorGBPerMovedGB ergibt geschätztes Log-Wachstum (GB).
    # Default 0.30 = konservativ (30% der moved GB als Log-Headroom). Bei sehr großen Moves ggf. höher setzen.
    [double]$LogGrowthFactorGBPerMovedGB = 0.30,

    # Mindest-Frei% für das LOG-Volume (separat). DBs deren Log-Volume darunter liegt, werden NICHT genutzt.
    [int]$MinLogFreePercent = 30,

    [string[]]$ExcludeDBs = @(),

    [string[]]$IncludeDBs = @()
)

# --- Helper: Resolve Volume info for a given file path (supports mount points) ---
function Get-VolumeInfoForPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    # Normalize path
    $p = $Path
    if (-not $p.EndsWith("\")) { $p += "\" }

    # Local disks only, capacity > 0
    $vols = Get-CimInstance Win32_Volume | Where-Object { $_.DriveType -eq 3 -and $_.Capacity -gt 0 }

    # Best match is the LONGEST volume mountpoint that is a prefix of the path
    $match = $vols |
        Where-Object { $p -like "$($_.Name)*" } |
        Sort-Object { $_.Name.Length } -Descending |
        Select-Object -First 1

    if (-not $match) {
        return [pscustomobject]@{
            Label      = "UNKNOWN"
            Name       = "UNKNOWN"
            CapacityGB = 0.0
            FreeGB     = 0.0
            FreePct    = 0
        }
    }

    $capGB  = [math]::Round($match.Capacity  / 1GB, 2)
    $freeGB = [math]::Round($match.FreeSpace / 1GB, 2)
    $pct    = if ($match.Capacity -gt 0) { [math]::Round(($match.FreeSpace / $match.Capacity) * 100, 0) } else { 0 }

    [pscustomobject]@{
        Label      = [string]$match.Label
        Name       = [string]$match.Name
        CapacityGB = $capGB
        FreeGB     = $freeGB
        FreePct    = [int]$pct
    }
}

# --- Helper: Safe sum ---
function Get-SafeSumMB {
    param([object[]]$Items)
    $s = ($Items | Measure-Object SizeMB -Sum).Sum
    if ($null -eq $s) { return 0.0 }
    return [double]$s
}

Write-Host "Distribute-Mailboxes start (WhatIf=$WhatIf)" -ForegroundColor Cyan

# === Target DBs auto-detect ===
$targetDBs = Get-MailboxDatabase -Status |
    Where-Object { -not $_.Recovery } |
    Select-Object -ExpandProperty Name

# Optional: restrict to IncludeDBs
if ($IncludeDBs -and $IncludeDBs.Count -gt 0) {
    $targetDBs = $targetDBs | Where-Object { $_ -in $IncludeDBs }
}

# Optional: exclude
if ($ExcludeDBs -and $ExcludeDBs.Count -gt 0) {
    $targetDBs = $targetDBs | Where-Object { $_ -notin $ExcludeDBs }
}

if (-not $targetDBs -or $targetDBs.Count -lt 1) {
    throw "Keine Ziel-DBs gefunden (nach Include/Exclude-Filter)."
}

# === DB -> Volume info mapping ===
$dbInfo = @{}
foreach ($dbName in @($targetDBs)) {
    $dbObj   = Get-MailboxDatabase -Identity $dbName -Status
    $edbPath = $dbObj.EdbFilePath.PathName
    $logPath = $null
    try { $logPath = $dbObj.LogFolderPath.PathName } catch { $logPath = $null }

    $vol = Get-VolumeInfoForPath -Path $edbPath
    $logVol = $null
    if ($logPath) {
        $logVol = Get-VolumeInfoForPath -Path $logPath
    } else {
        $logVol = [pscustomobject]@{ Label="UNKNOWN"; Name="UNKNOWN"; CapacityGB=0.0; FreeGB=0.0; FreePct=0 }
    }

    $dbSizeGB = 0.0
    $whitespaceGB = 0.0
    try {
        if ($dbObj.DatabaseSize) { $dbSizeGB = [math]::Round(($dbObj.DatabaseSize.ToBytes() / 1GB), 2) }
    } catch { $dbSizeGB = 0.0 }

    try {
        if ($dbObj.AvailableNewMailboxSpace) { $whitespaceGB = [math]::Round(($dbObj.AvailableNewMailboxSpace.ToBytes() / 1GB), 2) }
    } catch { $whitespaceGB = 0.0 }

    $dbInfo[$dbName] = [pscustomobject]@{
        DBName        = $dbName
        EdbPath       = $edbPath
        LogPath       = $logPath

        VolLabel      = $vol.Label
        VolName       = $vol.Name
        CapacityGB    = $vol.CapacityGB
        FreeGB        = $vol.FreeGB
        FreePct       = $vol.FreePct

        LogVolLabel   = $logVol.Label
        LogVolName    = $logVol.Name
        LogCapGB      = $logVol.CapacityGB
        LogFreeGB     = $logVol.FreeGB
        LogFreePct    = $logVol.FreePct

        DbSizeGB      = $dbSizeGB
        WhitespaceGB  = $whitespaceGB
    }
}


Write-Host "
DB/Volume Übersicht (Exchange-DBName ↔ Volume):" -ForegroundColor Cyan
$dbInfo.Values | Sort-Object FreePct | Format-Table DBName, VolLabel, FreePct, FreeGB, CapacityGB, LogVolLabel, LogFreePct, LogFreeGB, LogCapGB, DbSizeGB, WhitespaceGB, EdbPath -AutoSize

# === Filter DBs by MinFreePercent ===
$targetDBs = $targetDBs | Where-Object {
    ($dbInfo[$_].FreePct -ge $MinFreePercent) -and ($dbInfo[$_].LogFreePct -ge $MinLogFreePercent)
}

if (-not $targetDBs -or $targetDBs.Count -lt 1) {
    throw "Nach Filtern bleibt keine Ziel-DB übrig. MinFreePercent=$MinFreePercent (DB) / MinLogFreePercent=$MinLogFreePercent (LOG)."
}

Write-Host "
Ziel-DBs nach Filter (MinFreePercent=$MinFreePercent / MinLogFreePercent=$MinLogFreePercent): $($targetDBs -join ', ')" -ForegroundColor Cyan

# === Mailboxes sammeln (robust) ===
Write-Host "\nSammle UserMailbox-Mailboxen…" -ForegroundColor Cyan

$mailboxes = Get-Mailbox -ResultSize Unlimited -RecipientTypeDetails UserMailbox |
    ForEach-Object {
        $mbx = $_
        $stats = $null

        try {
            $stats = Get-MailboxStatistics -Identity $mbx.Identity -ErrorAction Stop -WarningAction SilentlyContinue
        } catch {
            Write-Warning "Keine MailboxStatistics für '$($mbx.Identity)' → SizeMB=0 (Grund: $($_.Exception.Message))"
        }

        $sizeMB = 0.0
        if ($stats -and $stats.TotalItemSize -and $stats.TotalItemSize.Value) {
            try {
                $sizeMB = [math]::Round($stats.TotalItemSize.Value.ToMB(), 2)
            } catch {
                $sizeMB = 0.0
            }
        }

        [pscustomobject]@{
            DisplayName = $mbx.DisplayName
            Identity    = $mbx.Identity
            SizeMB      = [double]$sizeMB
        }
    }

if (-not $mailboxes -or $mailboxes.Count -lt 1) {
    Write-Warning "Keine UserMailbox-Mailboxen gefunden (oder keine lesbaren Daten)."
    return
}

# Große Mailboxen zuerst verteilen (stabilere Ergebnisse)
$mailboxes = $mailboxes | Sort-Object SizeMB -Descending

# === Distributions initialisieren ===
$distributions = @{}
foreach ($db in $targetDBs) { $distributions[$db] = @() }

# === Disk-aware Greedy Distribution ===
Write-Host "\nVerteile Mailboxen (disk-aware)…" -ForegroundColor Cyan

foreach ($mb in $mailboxes) {

    $targetDB = ($distributions.GetEnumerator() | Sort-Object {
        $dbName = $_.Key

        $assignedCount = $_.Value.Count
        $assignedMB    = Get-SafeSumMB -Items $_.Value

        # projiziert nach Zuweisung dieser Mailbox
        $projCount = $assignedCount + 1
        $projMB    = $assignedMB + [double]$mb.SizeMB

        $freeGB = [double]$dbInfo[$dbName].FreeGB
        $freeMB = [math]::Max(1.0, $freeGB * 1024)

        $freePct = [double]$dbInfo[$dbName].FreePct

        $baseUsedPenalty = (100.0 - $freePct) * 1000.0
        $sizePressure    = ($projMB / $freeMB) * 1000000.0
        $countPressure   = $projCount * 50.0

        [double]($baseUsedPenalty + $sizePressure + $countPressure)

    })[0].Key

    $distributions[$targetDB] += $mb
}

# === Ausgabe: Vorschau-Verteilung ===
Write-Host "\nVorschau-Verteilung:" -ForegroundColor Cyan

$distributions.GetEnumerator() |
    Sort-Object Key |
    ForEach-Object {
        $dbName = $_.Key
        $count  = $_.Value.Count
        $sumMB  = Get-SafeSumMB -Items $_.Value

        $freePct = $dbInfo[$dbName].FreePct
        $freeGB  = $dbInfo[$dbName].FreeGB
        $volLbl  = $dbInfo[$dbName].VolLabel

        Write-Host ("`n==> {0}: {1} Postfächer, MoveSumme: {2} MB | Volume '{3}' Free: {4}% ({5} GB)" -f $dbName,$count,[math]::Round($sumMB,2),$volLbl,$freePct,$freeGB)
    }

# === Plan/Report Daten vorbereiten ===
$plan = foreach ($db in $targetDBs) {
    foreach ($m in $distributions[$db]) {
        $srcDb = $null
        try {
            $srcDb = (Get-Mailbox -Identity $m.Identity -ErrorAction Stop).Database
        } catch {
            $srcDb = "UNKNOWN"
        }

        [pscustomobject]@{
            DisplayName     = $m.DisplayName
            Identity        = [string]$m.Identity
            SourceDatabase  = [string]$srcDb
            TargetDatabase  = [string]$db
            SizeMB          = [double]$m.SizeMB

            TargetVolLabel  = [string]$dbInfo[$db].VolLabel
            TargetFreePct   = [int]$dbInfo[$db].FreePct
            TargetFreeGB    = [double]$dbInfo[$db].FreeGB

            LogVolLabel     = [string]$dbInfo[$db].LogVolLabel
            LogFreePct      = [int]$dbInfo[$db].LogFreePct
            LogFreeGB       = [double]$dbInfo[$db].LogFreeGB
        }
    }
}


# === Reports schreiben (CSV + HTML) ===
# (Default ReportRoot): immer relativ zum Script-Ordner (da wo das Skript liegt)
# -> Dadurch findest du die Reports zuverlässig, egal ob EMS/RunAs/Remoting.
if (-not $script:ReportRoot -or [string]::IsNullOrWhiteSpace($script:ReportRoot)) {
    $baseDir = $PSScriptRoot
    if (-not $baseDir) {
        # Fallback, falls ausnahmsweise kein Script-Kontext vorhanden ist
        $baseDir = (Get-Location).Path
    }

    $reportsDir = Join-Path -Path $baseDir -ChildPath "Reports"
    $script:ReportRoot = Join-Path -Path $reportsDir -ChildPath "Distribute-Mailboxes"
}

$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir   = Join-Path -Path $script:ReportRoot -ChildPath $runStamp
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$csvPath  = Join-Path $runDir "distribution-plan.csv"
$htmlPath = Join-Path $runDir "distribution-plan.html"

$plan | Sort-Object TargetDatabase, SizeMB -Descending | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath

# HTML: Summary + Tabellen
$summaryRows = foreach ($db in $targetDBs) {
    $rows = $plan | Where-Object TargetDatabase -eq $db
    $sum  = ($rows | Measure-Object SizeMB -Sum).Sum
    if ($null -eq $sum) { $sum = 0 }

    $capGB  = [double]$dbInfo[$db].CapacityGB
    $freeGB = [double]$dbInfo[$db].FreeGB
    $freePctBefore = [double]$dbInfo[$db].FreePct

    $moveSumGB = [math]::Round(([double]$sum / 1024), 2)

    # Wichtiger Check:
    # - Mailbox-Move "Größe" != 1:1 EDB-Wachstum
    # - Ziel-DB hat evtl. bereits Whitespace (AvailableNewMailboxSpace), der das Wachstum abfedert
    # -> Ttatsächliches notwendiges EDB/Volume-Wachstum als: max(0, MoveSumGB - WhitespaceGB) * SafetyFactor

    $whitespaceGB = 0.0
    try { $whitespaceGB = [double]$dbInfo[$db].WhitespaceGB } catch { $whitespaceGB = 0.0 }

    $safetyFactor = 1.10  # 10% Puffer (Header/Overhead/Unschärfe)
    $neededGrowthGB = [math]::Max(0.0, ($moveSumGB - $whitespaceGB)) * $safetyFactor
    $neededGrowthGB = [math]::Round($neededGrowthGB, 2)

    # Logs liegen separat:
    # Sehr grobe (aber praktische) Projektion für Log-Headroom während großer Moves.
    # Default: 30% der moved GB als zusätzlicher Log-Bedarf (anpassbar via -LogGrowthFactorGBPerMovedGB)
    $logNeededGB = [math]::Round(($moveSumGB * [double]$LogGrowthFactorGBPerMovedGB), 2)

    $projFreeGB = [math]::Round(($freeGB - $neededGrowthGB), 2)
    if ($projFreeGB -lt 0) { $projFreeGB = 0 }

    $projFreePct = if ($capGB -gt 0) { [math]::Round(($projFreeGB / $capGB) * 100, 0) } else { 0 }

    # Log-Projektion
    $logFreeGB = 0.0
    $logCapGB  = 0.0
    try { $logFreeGB = [double]$dbInfo[$db].LogFreeGB } catch { $logFreeGB = 0.0 }
    try { $logCapGB  = [double]$dbInfo[$db].LogCapGB } catch { $logCapGB = 0.0 }

    $projLogFreeGB = [math]::Round(($logFreeGB - $logNeededGB), 2)
    if ($projLogFreeGB -lt 0) { $projLogFreeGB = 0 }

    $projLogFreePct = if ($logCapGB -gt 0) { [math]::Round(($projLogFreeGB / $logCapGB) * 100, 0) } else { 0 }

    # Ampel für DB-Volume (EDB)
    $ampelDb = if ($projFreePct -lt $MinFreePercent) { "ROT" }
               elseif ($projFreePct -lt ($MinFreePercent + 10)) { "GELB" }
               else { "GRUEN" }

    # Ampel für LOG-Volume
    $ampelLog = if ($projLogFreePct -lt $MinLogFreePercent) { "ROT" }
                elseif ($projLogFreePct -lt ($MinLogFreePercent + 10)) { "GELB" }
                else { "GRUEN" }

    # Gesamtampel = schlechtester Wert
    $ampel = if ($ampelDb -eq "ROT" -or $ampelLog -eq "ROT") { "ROT" }
             elseif ($ampelDb -eq "GELB" -or $ampelLog -eq "GELB") { "GELB" }
             else { "GRUEN" }

    $ampel = if ($projFreePct -lt $MinFreePercent) { "ROT" }
             elseif ($projFreePct -lt ($MinFreePercent + 10)) { "GELB" }
             else { "GRUEN" }    [pscustomobject]@{
        TargetDatabase      = $db
        MoveCount           = $rows.Count
        MoveSumMB           = [math]::Round([double]$sum, 2)
        VolLabel            = $dbInfo[$db].VolLabel
        FreePctBefore       = [int]$freePctBefore
        FreeGBBefore        = [math]::Round($freeGB, 2)
        WhitespaceGB        = [math]::Round([double]$whitespaceGB, 2)
        NeededGrowthGB      = [math]::Round([double]$neededGrowthGB, 2)
        ProjectedFreePct    = [int]$projFreePct
        ProjectedFreeGB     = [math]::Round($projFreeGB, 2)

        AmpelDB            = $ampelDb
        AmpelLog           = $ampelLog

        LogVolLabel         = $dbInfo[$db].LogVolLabel
        LogFreePctBefore    = [int]$dbInfo[$db].LogFreePct
        LogFreeGBBefore     = [math]::Round([double]$dbInfo[$db].LogFreeGB, 2)
        LogNeededGB         = [math]::Round([double]$logNeededGB, 2)
        ProjectedLogFreePct = [int]$projLogFreePct
        ProjectedLogFreeGB  = [math]::Round($projLogFreeGB, 2)

        Ampel               = $ampel
    }
}

# KPIs
$totalMoves = $plan.Count
$totalMB = ($plan | Measure-Object SizeMB -Sum).Sum
if ($null -eq $totalMB) { $totalMB = 0 }
$totalGB = [math]::Round(([double]$totalMB / 1024), 2)

$minProjPct = ($summaryRows | Measure-Object ProjectedFreePct -Minimum).Minimum
$minProjDB  = ($summaryRows | Sort-Object ProjectedFreePct | Select-Object -First 1).TargetDatabase

# Top 10 größte Moves
$top10 = $plan | Sort-Object SizeMB -Descending | Select-Object -First 10
$top10Html = $top10 |
    Select-Object DisplayName, SourceDatabase, TargetDatabase, SizeMB |
    ConvertTo-Html -Fragment -PreContent "<h2>Top 10 größte Moves</h2>"

# Summary-Table 
$summaryTable = "<h2>DB Summary (vorher / projiziert nach Plan)</h2>" +
"<p class='small'>Hinweis: <b>projiziert</b> ist eine Schätzung des <b>zusätzlichen</b> Volume-Bedarfs der Ziel-DB: max(0, MoveSumGB - WhitespaceGB) x 1.10. Mailbox-Größe ist nicht 1:1 EDB-Wachstum; vorhandener Whitespace kann Wachstum abfedern. <br/>Logs liegen separat: geschätzter Log-Headroom = MoveSumGB x <b>$LogGrowthFactorGBPerMovedGB</b> (anpassbar via -LogGrowthFactorGBPerMovedGB).</p>" +
"<table><thead><tr>" +
"<th>Target DB</th><th>Moves</th><th>MoveSum (MB)</th><th>Volume</th><th>Free% vorher</th><th>FreeGB vorher</th><th>Whitespace (GB)</th><th>NeedGrowth (GB)</th><th>Free% proj.</th><th>FreeGB proj.</th><th>DB Ampel</th><th>LogVol</th><th>LogFree% vorher</th><th>LogFreeGB vorher</th><th>LogNeed (GB)</th><th>LogFree% proj.</th><th>LogFreeGB proj.</th><th>Log Ampel</th><th>Gesamt</th>" +
"</tr></thead><tbody>""</tr></thead><tbody>"

foreach ($r in ($summaryRows | Sort-Object ProjectedFreePct)) {
    $clsDb = switch ($r.AmpelDB) {
        "GRUEN" { "badge green" }
        "GELB"  { "badge yellow" }
        default { "badge red" }
    }

    $clsLog = switch ($r.AmpelLog) {
        "GRUEN" { "badge green" }
        "GELB"  { "badge yellow" }
        default { "badge red" }
    }

    $cls = switch ($r.Ampel) {
        "GRUEN" { "badge green" }
        "GELB"  { "badge yellow" }
        default { "badge red" }
    }

    $summaryTable += "<tr>" +
        "<td><b>$($r.TargetDatabase)</b></td>" +
        "<td>$($r.MoveCount)</td>" +
        "<td>$($r.MoveSumMB)</td>" +
        "<td>$($r.VolLabel)</td>" +
        "<td>$($r.FreePctBefore)%</td>" +
        "<td>$($r.FreeGBBefore)</td>" +
        "<td>$($r.WhitespaceGB)</td>" +
        "<td><b>$($r.NeededGrowthGB)</b></td>" +
        "<td><b>$($r.ProjectedFreePct)%</b></td>" +
        "<td><b>$($r.ProjectedFreeGB)</b></td>" +
        "<td><span class='$clsDb'>$($r.AmpelDB)</span></td>" +
        "<td>$($r.LogVolLabel)</td>" +
        "<td>$($r.LogFreePctBefore)%</td>" +
        "<td>$($r.LogFreeGBBefore)</td>" +
        "<td><b>$($r.LogNeededGB)</b></td>" +
        "<td><b>$($r.ProjectedLogFreePct)%</b></td>" +
        "<td><b>$($r.ProjectedLogFreeGB)</b></td>" +
        "<td><span class='$clsLog'>$($r.AmpelLog)</span></td>" +
        "<td><span class='$cls'>$($r.Ampel)</span></td>" +
        "</tr>"
}

$summaryTable += "</tbody></table>"

# Plan-Tabelle
$planHtml = $plan |
    Select-Object DisplayName, SourceDatabase, TargetDatabase, SizeMB |
    Sort-Object TargetDatabase, SizeMB -Descending |
    ConvertTo-Html -Fragment -PreContent "<h2>Plan (kompakt)</h2>"

# Vollplan-Tabelle (alle Spalten) – nützlich fürs Debugging
$fullPlanHtml = $plan |
    Sort-Object TargetDatabase, SizeMB -Descending |
    ConvertTo-Html -Fragment -PreContent "<h2>Plan (vollständig)</h2>"

$style = @"
<style>
body{font-family:Segoe UI,Arial,sans-serif;margin:24px;}
h1{margin-bottom:6px;}
h2{margin-top:24px;}
.small{color:#444;font-size:13px;}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:16px 0 10px;}
.kpi{border:1px solid #ddd;border-radius:10px;padding:12px;background:#fafafa;}
.kpi .label{font-size:12px;color:#666;margin-bottom:6px;}
.kpi .value{font-size:22px;font-weight:700;}
.kpi .sub{font-size:12px;color:#666;margin-top:6px;}

.badge{display:inline-block;padding:4px 10px;border-radius:999px;font-weight:700;font-size:12px;letter-spacing:0.5px;}
.badge.green{background:#e7f6ea;border:1px solid #8fd19e;color:#1f7a2f;}
.badge.yellow{background:#fff6dd;border:1px solid #f0c36d;color:#8a5a00;}
.badge.red{background:#fde8e8;border:1px solid #f2a3a3;color:#a11616;}

table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #ddd;padding:8px;font-size:13px;}
th{background:#f2f2f2;text-align:left;}
tr:nth-child(even){background:#fcfcfc;}
</style>
"@

@"
<html><head><meta charset='utf-8'>$style</head>
<body>
<h1>Distribute-Mailboxes Report</h1>
<p class='small'>
<b>Run:</b> $runStamp<br/>
<b>WhatIf:</b> $WhatIf<br/>
<b>MinFreePercent:</b> $MinFreePercent<br/>
<b>TargetDBs:</b> $($targetDBs -join ', ')
</p>

<div class='kpis'>
  <div class='kpi'><div class='label'>Gesamt Moves</div><div class='value'>$totalMoves</div><div class='sub'>geplant</div></div>
  <div class='kpi'><div class='label'>Gesamt Volumen</div><div class='value'>$totalGB GB</div><div class='sub'>($([math]::Round([double]$totalMB,2)) MB)</div></div>
  <div class='kpi'><div class='label'>Kritischster DB-Wert (proj.)</div><div class='value'>$minProjPct%</div><div class='sub'>DB: $minProjDB</div></div>
  <div class='kpi'><div class='label'>Schwelle</div><div class='value'>$MinFreePercent%</div><div class='sub'>unterhalb = ROT</div></div>
</div>

$top10Html
$summaryTable
$planHtml
$fullPlanHtml
</body></html>
"@ | Out-File -Encoding UTF8 -FilePath $htmlPath

Write-Host "
Report geschrieben:" -ForegroundColor Cyan
Write-Host "  CSV : $csvPath"
Write-Host "  HTML: $htmlPath"

# === Guardrails (Limits) ===
# 1) MaxMovesPerDB / MaxMoveSumMBPerDB (optional)
if (-not $script:MaxMovesPerDB)      { $script:MaxMovesPerDB = 0 }
if (-not $script:MaxMoveSumMBPerDB)  { $script:MaxMoveSumMBPerDB = 0 }

$violations = @()
foreach ($db in $targetDBs) {
    $rows = $plan | Where-Object TargetDatabase -eq $db
    $sum  = ($rows | Measure-Object SizeMB -Sum).Sum
    if ($null -eq $sum) { $sum = 0 }

    if ($script:MaxMovesPerDB -gt 0 -and $rows.Count -gt $script:MaxMovesPerDB) {
        $violations += "DB '$db' hat $($rows.Count) Moves > MaxMovesPerDB=$($script:MaxMovesPerDB)"
    }
    if ($script:MaxMoveSumMBPerDB -gt 0 -and [double]$sum -gt [double]$script:MaxMoveSumMBPerDB) {
        $violations += "DB '$db' hat MoveSumMB=$([math]::Round([double]$sum,2)) > MaxMoveSumMBPerDB=$($script:MaxMoveSumMBPerDB)"
    }
}

if ($violations.Count -gt 0) {
    Write-Host "
GUARDRAIL TRIGGERED – Abbruch, weil Limits überschritten:" -ForegroundColor Red
    $violations | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    throw "Guardrails verletzt. Passe Limits/Filter an oder reduziere Zielmenge."
}

# === Optional: MoveRequests erstellen ===
Write-Host "
MoveRequests:" -ForegroundColor Cyan

foreach ($db in $targetDBs) {
    foreach ($m in $distributions[$db]) {
        $msg = "Move '$($m.DisplayName)' nach $db ($($m.SizeMB) MB)"

        if ($WhatIf) {
            Write-Host "[WhatIf] $msg"
            continue
        }

        # Falls es schon einen MoveRequest gibt, überspringen
        $existing = Get-MoveRequest -Identity $m.Identity -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Warning "Überspringe '$($m.DisplayName)': MoveRequest existiert bereits (Status: $($existing.Status))"
            continue
        }

        Write-Host "Starte $msg"
        New-MoveRequest -Identity $m.Identity -TargetDatabase $db -SuspendWhenReadyToComplete
    }
}

Write-Host "
Done." -ForegroundColor Green