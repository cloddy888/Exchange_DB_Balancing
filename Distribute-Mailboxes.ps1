<#
Distribute-Mailboxes.ps1

.SYNOPSIS
  Sammelt UserMailbox-Postfächer, ermittelt die Größe (MB) und verteilt sie BALANCIERT auf Ziel-Mailboxdatenbanken.
  Berücksichtigt DB-Volume (EDB) UND Log-Volume (separat) mit harten Guardrails (auch projiziert).

.DESCRIPTION
  - Holt alle UserMailbox-Mailboxen inkl. Größe (Get-MailboxStatistics).
  - Ermittelt je Mailbox-DB freien Speicher des DB-Volumes (EDB) und Log-Volumes (Win32_Volume) anhand der Pfade.
  - Filtert Ziel-DBs nach Mindest-Free% (DB) und Mindest-Free% (LOG) + Include/Exclude.
  - Balanced-Verteilung: minimiert die "vollste" DB (minimax) und bei Gleichstand die Varianz (gleichmäßiger).
  - Harte Constraints werden NACH jeder Zuweisung geprüft:
      Projected DB Free%  >= MinFreePercent
      Projected LOG Free% >= MinLogFreePercent
  - Guardrails sind IMMER aktiv (Hard-Constraints + Final-Check + optionale Zusatzlimits).

  Hinweis:
  - Mailboxen ohne verwertbare Statistics (z.B. nie angemeldet) bekommen SizeMB=0.
  - MoveRequests werden mit -SuspendWhenReadyToComplete erstellt (Cutover später im Wartungsfenster).

.PARAMETER WhatIf
  Simulation: Es werden keine MoveRequests erstellt, nur Verteilung + Reports.

.PARAMETER MinFreePercent
  Mindest-Frei% auf dem DB-Volume (EDB) – Default: 35

.PARAMETER MinLogFreePercent
  Mindest-Frei% auf dem LOG-Volume – Default: 30

.PARAMETER LogGrowthFactorGBPerMovedGB
  Grobe Log-Headroom-Projektion: MoveSumGB * Faktor – Default: 0.30

.PARAMETER BatchSize / BatchNumber
  Batching/Wellen: z.B. BatchSize 200, BatchNumber 1..n (0 = kein batching)

.PARAMETER MaxTotalMoveGB
  Optional: pro Run nur bis X GB Gesamtmove planen (0 = aus)

.PARAMETER MaxMovesPerDB / MaxMoveSumMBPerDB
  Optionale Zusatzlimits (immer geprüft, aber 0 = "kein Limit")
#>

[CmdletBinding()]
param(
    [switch]$WhatIf,

    [int]$MinFreePercent = 35,
    [int]$MinLogFreePercent = 30,
    [double]$LogGrowthFactorGBPerMovedGB = 0.30,

    [string[]]$ExcludeDBs = @(),
    [string[]]$IncludeDBs = @(),

    # --- optionale Zusatzlimits (immer geprüft, aber 0 = "kein Limit") ---
    [int]$MaxMovesPerDB = 0,
    [double]$MaxMoveSumMBPerDB = 0,

    # --- Batch/Wellensteuerung ---
    [int]$BatchSize = 0,
    [int]$BatchNumber = 1,
    [double]$MaxTotalMoveGB = 0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# -----------------------------
# Helpers
# -----------------------------

function Get-VolumeInfoForPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    $p = $Path
    if (-not $p.EndsWith("\")) { $p += "\" }

    $vols = Get-CimInstance Win32_Volume | Where-Object { $_.DriveType -eq 3 -and $_.Capacity -gt 0 }

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
        CapacityGB = [double]$capGB
        FreeGB     = [double]$freeGB
        FreePct    = [int]$pct
    }
}

function Get-SafeSumMB {
    param([object[]]$Items)
    $s = ($Items | Measure-Object SizeMB -Sum).Sum
    if ($null -eq $s) { return 0.0 }
    return [double]$s
}

function AmpelFromPct {
    param(
        [int]$Pct,
        [int]$Min
    )
    if ($Pct -lt $Min) { return "ROT" }
    elseif ($Pct -lt ($Min + 10)) { return "GELB" }
    else { return "GRUEN" }
}

# -----------------------------
# Start / Params
# -----------------------------

Write-Host "Distribute-Mailboxes start (WhatIf=$WhatIf)" -ForegroundColor Cyan
Write-Host ("Params: MinFreePercent(DB)={0} / MinLogFreePercent(LOG)={1} / LogGrowthFactor={2} / BatchSize={3} / BatchNumber={4} / MaxTotalMoveGB={5} / MaxMovesPerDB={6} / MaxMoveSumMBPerDB={7}" -f `
    $MinFreePercent,$MinLogFreePercent,$LogGrowthFactorGBPerMovedGB,$BatchSize,$BatchNumber,$MaxTotalMoveGB,$MaxMovesPerDB,$MaxMoveSumMBPerDB) -ForegroundColor DarkCyan

if ($BatchNumber -lt 1) { throw "BatchNumber muss >= 1 sein." }
if ($BatchSize -lt 0)   { throw "BatchSize darf nicht negativ sein." }

# -----------------------------
# DB discovery (ALL DBs)
# -----------------------------

$allDBs = Get-MailboxDatabase -Status |
    Where-Object { -not $_.Recovery } |
    Select-Object -ExpandProperty Name

if (-not $allDBs -or $allDBs.Count -lt 1) {
    throw "Keine Mailboxdatenbanken gefunden."
}

$targetCandidates = $allDBs

if ($IncludeDBs -and $IncludeDBs.Count -gt 0) {
    $targetCandidates = $targetCandidates | Where-Object { $_ -in $IncludeDBs }
}
if ($ExcludeDBs -and $ExcludeDBs.Count -gt 0) {
    $targetCandidates = $targetCandidates | Where-Object { $_ -notin $ExcludeDBs }
}
if (-not $targetCandidates -or $targetCandidates.Count -lt 1) {
    throw "Keine Ziel-DBs gefunden (nach Include/Exclude-Filter)."
}

# -----------------------------
# DB -> Volume mapping (ALL DBs)
# -----------------------------

$dbInfo = @{}
foreach ($dbName in @($allDBs)) {
    $dbObj = Get-MailboxDatabase -Identity $dbName -Status

    $edbPath = $dbObj.EdbFilePath.PathName
    $logPath = $null
    try { $logPath = $dbObj.LogFolderPath.PathName } catch { $logPath = $null }

    $vol = Get-VolumeInfoForPath -Path $edbPath
    $logVol = if ($logPath) { Get-VolumeInfoForPath -Path $logPath } else { [pscustomobject]@{ Label="UNKNOWN"; Name="UNKNOWN"; CapacityGB=0.0; FreeGB=0.0; FreePct=0 } }

    $dbSizeGB = 0.0
    $whitespaceGB = 0.0
    try { if ($dbObj.DatabaseSize) { $dbSizeGB = [math]::Round(($dbObj.DatabaseSize.ToBytes() / 1GB), 2) } } catch {}
    try { if ($dbObj.AvailableNewMailboxSpace) { $whitespaceGB = [math]::Round(($dbObj.AvailableNewMailboxSpace.ToBytes() / 1GB), 2) } } catch {}

    $dbInfo[$dbName] = [pscustomobject]@{
        DBName        = $dbName
        EdbPath       = $edbPath
        LogPath       = $logPath

        VolLabel      = $vol.Label
        VolName       = $vol.Name
        CapacityGB    = [double]$vol.CapacityGB
        FreeGB        = [double]$vol.FreeGB
        FreePct       = [int]$vol.FreePct

        LogVolLabel   = $logVol.Label
        LogVolName    = $logVol.Name
        LogCapGB      = [double]$logVol.CapacityGB
        LogFreeGB     = [double]$logVol.FreeGB
        LogFreePct    = [int]$logVol.FreePct

        DbSizeGB      = [double]$dbSizeGB
        WhitespaceGB  = [double]$whitespaceGB
    }
}

Write-Host "`nDB/Volume Übersicht (ALL DBs):" -ForegroundColor Cyan
$dbInfo.Values |
    Sort-Object FreePct |
    Format-Table DBName, VolLabel, FreePct, FreeGB, CapacityGB, LogVolLabel, LogFreePct, LogFreeGB, LogCapGB, DbSizeGB, WhitespaceGB -AutoSize

# -----------------------------
# Target filter by thresholds (NOW)
# -----------------------------

$targetDBs = $targetCandidates | Where-Object {
    ($dbInfo[$_].FreePct -ge $MinFreePercent) -and ($dbInfo[$_].LogFreePct -ge $MinLogFreePercent)
}

if (-not $targetDBs -or $targetDBs.Count -lt 1) {
    throw "Nach Filtern bleibt keine Ziel-DB übrig. MinFreePercent=$MinFreePercent (DB) / MinLogFreePercent=$MinLogFreePercent (LOG)."
}

Write-Host ("`nZiel-DBs nach Filter: {0}" -f ($targetDBs -join ', ')) -ForegroundColor Cyan

# -----------------------------
# Mailboxes sammeln
# -----------------------------

Write-Host "`nSammle UserMailbox-Mailboxen…" -ForegroundColor Cyan

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
            try { $sizeMB = [math]::Round($stats.TotalItemSize.Value.ToMB(), 2) } catch { $sizeMB = 0.0 }
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

$mailboxes = $mailboxes | Sort-Object SizeMB -Descending

# -----------------------------
# Batching / MaxTotalMoveGB
# -----------------------------

$allCount = $mailboxes.Count

if ($BatchSize -gt 0) {
    $start = ($BatchNumber - 1) * $BatchSize
    if ($start -ge $allCount) {
        throw "Batch $BatchNumber existiert nicht. ($allCount Mailboxen, BatchSize=$BatchSize)"
    }
    $mailboxes = $mailboxes | Select-Object -Skip $start -First $BatchSize
    Write-Host ("`nBatching aktiv: Batch {0} (Size={1}) → verarbeitet {2} von {3} Mailboxen" -f $BatchNumber,$BatchSize,$mailboxes.Count,$allCount) -ForegroundColor Cyan
} else {
    Write-Host ("`nBatching aus: verarbeitet alle {0} Mailboxen" -f $allCount) -ForegroundColor Cyan
}

if ($MaxTotalMoveGB -gt 0) {
    $sumGB = 0.0
    $picked = New-Object System.Collections.Generic.List[object]
    foreach ($m in $mailboxes) {
        $mGB = ([double]$m.SizeMB / 1024)
        if (($sumGB + $mGB) -gt $MaxTotalMoveGB) { break }
        $picked.Add($m) | Out-Null
        $sumGB += $mGB
    }
    $mailboxes = $picked
    Write-Host ("MaxTotalMoveGB aktiv: ausgewählt {0} Mailboxen (~{1} GB)" -f $mailboxes.Count,[math]::Round($sumGB,2)) -ForegroundColor Cyan
}

# -----------------------------
# Distribution state (EDB + LOG projection)
# -----------------------------

$distributions = @{}
foreach ($db in $targetDBs) { $distributions[$db] = @() }

$dbState = @{}
foreach ($db in $targetDBs) {
    $dbState[$db] = [pscustomobject]@{
        CapGB          = [double]$dbInfo[$db].CapacityGB
        FreeGB0        = [double]$dbInfo[$db].FreeGB
        LogCapGB       = [double]$dbInfo[$db].LogCapGB
        LogFreeGB0     = [double]$dbInfo[$db].LogFreeGB
        WhitespaceGB0  = [double]$dbInfo[$db].WhitespaceGB
        SafetyFactor   = 1.10
        AssignedMoveGB = 0.0
        NeededGrowthGB = 0.0
        LogNeededGB    = 0.0
    }
}

function Get-ProjectedAfter {
    param(
        [Parameter(Mandatory)][string]$DatabaseName,
        [Parameter(Mandatory)][double]$AddMoveGB
    )

    $s = $dbState[$DatabaseName]
    $newAssigned = $s.AssignedMoveGB + $AddMoveGB

    $newNeededGrowth = [math]::Max(0.0, ($newAssigned - $s.WhitespaceGB0)) * $s.SafetyFactor
    $newLogNeeded    = $s.LogNeededGB + ($AddMoveGB * [double]$LogGrowthFactorGBPerMovedGB)

    $projFreeGB = $s.FreeGB0 - $newNeededGrowth
    if ($projFreeGB -lt 0) { $projFreeGB = 0 }
    $projFreePct = if ($s.CapGB -gt 0) { [math]::Round(($projFreeGB / $s.CapGB) * 100, 0) } else { 0 }

    $projLogFreeGB = $s.LogFreeGB0 - $newLogNeeded
    if ($projLogFreeGB -lt 0) { $projLogFreeGB = 0 }
    $projLogFreePct = if ($s.LogCapGB -gt 0) { [math]::Round(($projLogFreeGB / $s.LogCapGB) * 100, 0) } else { 0 }

    [pscustomobject]@{
        NewAssignedMoveGB = [double]$newAssigned
        NewNeededGrowthGB = [double]$newNeededGrowth
        NewLogNeededGB    = [double]$newLogNeeded
        ProjFreeGB        = [double]([math]::Round($projFreeGB, 2))
        ProjFreePct       = [int]$projFreePct
        ProjLogFreeGB     = [double]([math]::Round($projLogFreeGB, 2))
        ProjLogFreePct    = [int]$projLogFreePct
    }
}

# -----------------------------
# Balanced Distribution (minimax + variance) with hard constraints
# -----------------------------

Write-Host "`nVerteile Mailboxen (balanced, disk+log aware)..." -ForegroundColor Cyan

foreach ($mb in $mailboxes) {
    $mbGB = [math]::Round(([double]$mb.SizeMB / 1024), 4)

    $bestDb = $null
    $bestMaxUsed = [double]::PositiveInfinity
    $bestVar = [double]::PositiveInfinity

    foreach ($db in $targetDBs) {
        $cand = Get-ProjectedAfter -DatabaseName $db -AddMoveGB $mbGB

        if ($cand.ProjFreePct -lt $MinFreePercent) { continue }
        if ($cand.ProjLogFreePct -lt $MinLogFreePercent) { continue }

        $usedPcts = @()
        foreach ($d in $targetDBs) {
            if ($d -eq $db) {
                $usedPcts += (100.0 - [double]$cand.ProjFreePct)
            } else {
                $cur = Get-ProjectedAfter -DatabaseName $d -AddMoveGB 0
                $usedPcts += (100.0 - [double]$cur.ProjFreePct)
            }
        }

        $maxUsed  = ($usedPcts | Measure-Object -Maximum).Maximum
        $meanUsed = ($usedPcts | Measure-Object -Average).Average

        $var = 0.0
        foreach ($u in $usedPcts) { $var += [math]::Pow(([double]$u - [double]$meanUsed), 2) }

        if ($maxUsed -lt $bestMaxUsed -or ($maxUsed -eq $bestMaxUsed -and $var -lt $bestVar)) {
            $bestDb = $db
            $bestMaxUsed = [double]$maxUsed
            $bestVar = [double]$var
        }
    }

    if (-not $bestDb) {
        throw "Für Mailbox '$($mb.DisplayName)' ($($mb.SizeMB) MB) konnte keine Ziel-DB gefunden werden, die die Constraints erfüllt (MinFreePercent=$MinFreePercent / MinLogFreePercent=$MinLogFreePercent)."
    }

    $candFinal = Get-ProjectedAfter -DatabaseName $bestDb -AddMoveGB $mbGB
    $st = $dbState[$bestDb]
    $st.AssignedMoveGB = $candFinal.NewAssignedMoveGB
    $st.NeededGrowthGB = $candFinal.NewNeededGrowthGB
    $st.LogNeededGB    = $candFinal.NewLogNeededGB

    $distributions[$bestDb] += $mb
}

# -----------------------------
# Plan objects
# -----------------------------

Write-Host "`nVorschau-Verteilung:" -ForegroundColor Cyan

$distributions.GetEnumerator() |
    Sort-Object Key |
    ForEach-Object {
        $dbName = $_.Key
        $count  = $_.Value.Count
        $sumMB  = Get-SafeSumMB -Items $_.Value

        Write-Host ("`n==> {0}: {1} Postfächer, MoveSumme: {2} MB | DBVol '{3}' Free: {4}% ({5} GB) | LogVol '{6}' Free: {7}% ({8} GB)" -f `
            $dbName,$count,[math]::Round($sumMB,2),
            $dbInfo[$dbName].VolLabel,$dbInfo[$dbName].FreePct,$dbInfo[$dbName].FreeGB,
            $dbInfo[$dbName].LogVolLabel,$dbInfo[$dbName].LogFreePct,$dbInfo[$dbName].LogFreeGB)
    }

$plan = foreach ($db in $targetDBs) {
    foreach ($m in $distributions[$db]) {
        $srcDb = "UNKNOWN"
        try { $srcDb = (Get-Mailbox -Identity $m.Identity -ErrorAction Stop).Database } catch {}

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

# -----------------------------
# Guardrails (ALWAYS ON)
# -----------------------------

$violations = @()

foreach ($db in $targetDBs) {
    $rows = $plan | Where-Object TargetDatabase -eq $db
    $sum  = ($rows | Measure-Object SizeMB -Sum).Sum
    if ($null -eq $sum) { $sum = 0 }

    if ($MaxMovesPerDB -gt 0 -and $rows.Count -gt $MaxMovesPerDB) {
        $violations += "DB '$db' hat $($rows.Count) Moves > MaxMovesPerDB=$MaxMovesPerDB"
    }
    if ($MaxMoveSumMBPerDB -gt 0 -and [double]$sum -gt [double]$MaxMoveSumMBPerDB) {
        $violations += "DB '$db' hat MoveSumMB=$([math]::Round([double]$sum,2)) > MaxMoveSumMBPerDB=$MaxMoveSumMBPerDB"
    }
}

foreach ($db in $targetDBs) {
    $st = $dbState[$db]

    $projDbFreeGB  = [math]::Max(0.0, ($st.FreeGB0 - $st.NeededGrowthGB))
    $projDbFreePct = if ($st.CapGB -gt 0) { [math]::Round(($projDbFreeGB / $st.CapGB) * 100, 0) } else { 0 }

    $projLogFreeGB  = [math]::Max(0.0, ($st.LogFreeGB0 - $st.LogNeededGB))
    $projLogFreePct = if ($st.LogCapGB -gt 0) { [math]::Round(($projLogFreeGB / $st.LogCapGB) * 100, 0) } else { 0 }

    if ([int]$projDbFreePct -lt $MinFreePercent) {
        $violations += "DB '$db' projiziert $projDbFreePct% < MinFreePercent=$MinFreePercent"
    }
    if ([int]$projLogFreePct -lt $MinLogFreePercent) {
        $violations += "LOG '$db' projiziert $projLogFreePct% < MinLogFreePercent=$MinLogFreePercent"
    }
}

if ($violations.Count -gt 0) {
    Write-Host "`nGUARDRAIL TRIGGERED – Abbruch:" -ForegroundColor Red
    $violations | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    throw "Guardrails verletzt."
}

# -----------------------------
# Reports (CSV + HTML)
# -----------------------------

$baseDir = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
$reportRoot = Join-Path -Path $baseDir -ChildPath "Reports\Distribute-Mailboxes"
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir   = Join-Path -Path $reportRoot -ChildPath $runStamp
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$csvPath  = Join-Path $runDir "distribution-plan.csv"
$htmlPath = Join-Path $runDir "distribution-plan.html"

$plan | Sort-Object TargetDatabase, SizeMB -Descending | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath

# -----------------------------
# Summary Rows (Targets)
# -----------------------------

$summaryRows = foreach ($db in $targetDBs) {
    $rows = $plan | Where-Object TargetDatabase -eq $db
    $sumMB = ($rows | Measure-Object SizeMB -Sum).Sum
    if ($null -eq $sumMB) { $sumMB = 0 }

    $capGB  = [double]$dbInfo[$db].CapacityGB
    $freeGB = [double]$dbInfo[$db].FreeGB
    $freePctBefore = [int]$dbInfo[$db].FreePct

    $moveSumGB = [double]([math]::Round(($sumMB / 1024), 2))
    $whitespaceGB = [double]$dbInfo[$db].WhitespaceGB
    $safetyFactor = 1.10

    $neededGrowthGB = [double]([math]::Round(([math]::Max(0.0, ($moveSumGB - $whitespaceGB)) * $safetyFactor), 2))

    $projFreeGB = [double]([math]::Round(($freeGB - $neededGrowthGB), 2))
    if ($projFreeGB -lt 0) { $projFreeGB = 0 }
    $projFreePct = if ($capGB -gt 0) { [int]([math]::Round(($projFreeGB / $capGB) * 100, 0)) } else { 0 }

    $logFreeGB = [double]$dbInfo[$db].LogFreeGB
    $logCapGB  = [double]$dbInfo[$db].LogCapGB
    $logFreePctBefore = [int]$dbInfo[$db].LogFreePct

    $logNeededGB = [double]([math]::Round(($moveSumGB * [double]$LogGrowthFactorGBPerMovedGB), 2))

    $projLogFreeGB = [double]([math]::Round(($logFreeGB - $logNeededGB), 2))
    if ($projLogFreeGB -lt 0) { $projLogFreeGB = 0 }
    $projLogFreePct = if ($logCapGB -gt 0) { [int]([math]::Round(($projLogFreeGB / $logCapGB) * 100, 0)) } else { 0 }

    $ampelDb  = AmpelFromPct -Pct $projFreePct -Min $MinFreePercent
    $ampelLog = AmpelFromPct -Pct $projLogFreePct -Min $MinLogFreePercent

    $ampel = if ($ampelDb -eq "ROT" -or $ampelLog -eq "ROT") { "ROT" }
             elseif ($ampelDb -eq "GELB" -or $ampelLog -eq "GELB") { "GELB" }
             else { "GRUEN" }

    [pscustomobject]@{
        TargetDatabase      = $db
        MoveCount           = $rows.Count
        MoveSumMB           = [double]([math]::Round($sumMB, 2))

        VolLabel            = $dbInfo[$db].VolLabel
        FreePctBefore       = $freePctBefore
        FreeGBBefore        = [double]([math]::Round($freeGB, 2))
        WhitespaceGB        = [double]([math]::Round($whitespaceGB, 2))
        NeededGrowthGB      = [double]([math]::Round($neededGrowthGB, 2))
        ProjectedFreePct    = $projFreePct
        ProjectedFreeGB     = [double]([math]::Round($projFreeGB, 2))
        AmpelDB             = $ampelDb

        LogVolLabel         = $dbInfo[$db].LogVolLabel
        LogFreePctBefore    = $logFreePctBefore
        LogFreeGBBefore     = [double]([math]::Round($logFreeGB, 2))
        LogNeededGB         = [double]([math]::Round($logNeededGB, 2))
        ProjectedLogFreePct = $projLogFreePct
        ProjectedLogFreeGB  = [double]([math]::Round($projLogFreeGB, 2))
        AmpelLog            = $ampelLog

        Ampel               = $ampel
    }
}

# -----------------------------
# All DBs overview (Non-targets included)
# -----------------------------

$reasonOrder = @{
    "TARGET"            = 0
    "EXCLUDED"          = 10
    "NOT_INCLUDED"      = 20
    "DB_FREE_TOO_LOW"   = 30
    "LOG_FREE_TOO_LOW"  = 40
    "OTHER"             = 999
}

$allDbRows = foreach ($db in $allDBs) {
    $isIncludedOk = $true
    if ($IncludeDBs -and $IncludeDBs.Count -gt 0) { $isIncludedOk = ($db -in $IncludeDBs) }

    $isExcluded = $false
    if ($ExcludeDBs -and $ExcludeDBs.Count -gt 0) { $isExcluded = ($db -in $ExcludeDBs) }

    $reason = "TARGET"
    $isTarget = $false

    if (-not $isIncludedOk) {
        $reason = "NOT_INCLUDED"
    } elseif ($isExcluded) {
        $reason = "EXCLUDED"
    } elseif ($dbInfo[$db].FreePct -lt $MinFreePercent) {
        $reason = "DB_FREE_TOO_LOW"
    } elseif ($dbInfo[$db].LogFreePct -lt $MinLogFreePercent) {
        $reason = "LOG_FREE_TOO_LOW"
    } elseif ($db -in $targetDBs) {
        $reason = "TARGET"
        $isTarget = $true
    } else {
        $reason = "OTHER"
    }

    $needGrowth = 0.0
    $dbFreeProjPct = [int]$dbInfo[$db].FreePct
    $dbFreeProjGB  = [double]$dbInfo[$db].FreeGB
    $logNeed = 0.0
    $logFreeProjPct = [int]$dbInfo[$db].LogFreePct
    $logFreeProjGB  = [double]$dbInfo[$db].LogFreeGB

    if ($isTarget) {
        $st = $dbState[$db]
        $needGrowth = [double]([math]::Round($st.NeededGrowthGB, 2))

        $projDbFreeGB = [math]::Max(0.0, ($st.FreeGB0 - $st.NeededGrowthGB))
        $dbFreeProjGB = [double]([math]::Round($projDbFreeGB, 2))
        $dbFreeProjPct = if ($st.CapGB -gt 0) { [int]([math]::Round(($projDbFreeGB / $st.CapGB) * 100, 0)) } else { 0 }

        $logNeed = [double]([math]::Round($st.LogNeededGB, 2))
        $projLogFreeGB = [math]::Max(0.0, ($st.LogFreeGB0 - $st.LogNeededGB))
        $logFreeProjGB = [double]([math]::Round($projLogFreeGB, 2))
        $logFreeProjPct = if ($st.LogCapGB -gt 0) { [int]([math]::Round(($projLogFreeGB / $st.LogCapGB) * 100, 0)) } else { 0 }
    }

    [pscustomobject]@{
        DB                = $db
        IsTarget          = if ($isTarget) { "YES" } else { "NO" }
        Reason            = $reason
        ReasonOrder       = [int]$reasonOrder[$reason]

        DBVol             = $dbInfo[$db].VolLabel
        DbFreePctBefore   = [int]$dbInfo[$db].FreePct
        DbFreeGBBefore    = [double]([math]::Round($dbInfo[$db].FreeGB, 2))
        WhitespaceGB      = [double]([math]::Round($dbInfo[$db].WhitespaceGB, 2))
        NeedGrowthGB      = [double]([math]::Round($needGrowth, 2))
        DbFreePctProj     = [int]$dbFreeProjPct
        DbFreeGBProj      = [double]([math]::Round($dbFreeProjGB, 2))

        LogVol            = $dbInfo[$db].LogVolLabel
        LogFreePctBefore  = [int]$dbInfo[$db].LogFreePct
        LogFreeGBBefore   = [double]([math]::Round($dbInfo[$db].LogFreeGB, 2))
        LogNeedGB         = [double]([math]::Round($logNeed, 2))
        LogFreePctProj    = [int]$logFreeProjPct
        LogFreeGBProj     = [double]([math]::Round($logFreeProjGB, 2))
    }
}

# -----------------------------
# Source Impact (Entlastung) – extra table
# -----------------------------

[double]$ExpectedWhitespaceFactor = 1.00

$sourceImpactRows = $plan |
    Group-Object SourceDatabase |
    ForEach-Object {
        $src = $_.Name
        $movesOut = $_.Count
        $sumMB = ($_.Group | Measure-Object SizeMB -Sum).Sum
        if ($null -eq $sumMB) { $sumMB = 0 }

        $sumGB = [math]::Round(([double]$sumMB / 1024), 2)
        $expWsGB = [math]::Round(($sumGB * $ExpectedWhitespaceFactor), 2)

        [pscustomobject]@{
            SourceDB                 = $src
            MovesOut                 = $movesOut
            MoveOutGB                = $sumGB
            ExpectedWhitespaceGainGB  = $expWsGB
        }
    } |
    Sort-Object MoveOutGB -Descending

# -----------------------------
# KPIs
# -----------------------------

$totalMoves = $plan.Count
$totalMB = ($plan | Measure-Object SizeMB -Sum).Sum
if ($null -eq $totalMB) { $totalMB = 0 }
$totalGB = [double]([math]::Round(($totalMB / 1024), 2))

$minProjDbPct = ($summaryRows | Measure-Object ProjectedFreePct -Minimum).Minimum
$minProjDbName = ($summaryRows | Sort-Object ProjectedFreePct | Select-Object -First 1).TargetDatabase

$minProjLogPct = ($summaryRows | Measure-Object ProjectedLogFreePct -Minimum).Minimum
$minProjLogName = ($summaryRows | Sort-Object ProjectedLogFreePct | Select-Object -First 1).TargetDatabase

$biggestSource = $sourceImpactRows | Select-Object -First 1
$biggestSourceName = if ($biggestSource) { $biggestSource.SourceDB } else { "-" }
$biggestSourceGB   = if ($biggestSource) { $biggestSource.MoveOutGB } else { 0 }

$top10 = $plan | Sort-Object SizeMB -Descending | Select-Object -First 10
$top10Html = $top10 |
    Select-Object DisplayName, SourceDatabase, TargetDatabase, SizeMB |
    ConvertTo-Html -Fragment -PreContent "<h2>Top 10 größte Moves</h2>"

# -----------------------------
# HTML tables
# -----------------------------

# Summary (Targets)
$summaryTable = @()
$summaryTable += "<h2>DB Summary (Targets) – vorher / projiziert nach Plan</h2>"
$summaryTable += "<p class='small'>Hinweis: <b>projiziert</b> ist eine Schätzung des <b>zusätzlichen</b> DB-Volume-Bedarfs: max(0, MoveSumGB - WhitespaceGB) × 1.10. Logs separat: LogNeed = MoveSumGB × <b>$LogGrowthFactorGBPerMovedGB</b>.</p>"
$summaryTable += "<table><thead><tr>"
$summaryTable += "<th>Target DB</th><th>Moves</th><th>MoveSum (MB)</th><th>Volume</th><th>Free% vorher</th><th>FreeGB vorher</th><th>Whitespace (GB)</th><th>NeedGrowth (GB)</th><th>Free% proj.</th><th>FreeGB proj.</th><th>DB Ampel</th>"
$summaryTable += "<th>LogVol</th><th>LogFree% vorher</th><th>LogFreeGB vorher</th><th>LogNeed (GB)</th><th>LogFree% proj.</th><th>LogFreeGB proj.</th><th>Log Ampel</th><th>Gesamt</th>"
$summaryTable += "</tr></thead><tbody>"

foreach ($r in ($summaryRows | Sort-Object ProjectedFreePct)) {
    $clsDb  = if ($r.AmpelDB  -eq "GRUEN") { "badge green" } elseif ($r.AmpelDB  -eq "GELB") { "badge yellow" } else { "badge red" }
    $clsLog = if ($r.AmpelLog -eq "GRUEN") { "badge green" } elseif ($r.AmpelLog -eq "GELB") { "badge yellow" } else { "badge red" }
    $clsAll = if ($r.Ampel    -eq "GRUEN") { "badge green" } elseif ($r.Ampel    -eq "GELB") { "badge yellow" } else { "badge red" }

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
        "<td><span class='$clsAll'>$($r.Ampel)</span></td>" +
        "</tr>"
}
$summaryTable += "</tbody></table>"
$summaryTableHtml = ($summaryTable -join "`n")

# All DBs overview (EXTRA TABLE) – FIXED SORT
$allDbsTable = @()
$allDbsTable += "<h2>Alle DBs – Übersicht (auch Nicht-Targets)</h2>"
$allDbsTable += "<p class='small'>Diese Tabelle zeigt <b>alle</b> DBs inkl. Projektion. Für Nicht-Targets ist Projektion = aktueller Stand. Spalte <b>Reason</b> zeigt, warum eine DB nicht als Target genutzt wurde.</p>"
$allDbsTable += "<table><thead><tr>"
$allDbsTable += "<th>DB</th><th>IsTarget</th><th>Reason</th><th>DBVol</th><th>DB Free% vorher</th><th>DB FreeGB vorher</th><th>Whitespace (GB)</th><th>NeedGrowth (GB)</th><th>DB Free% proj.</th><th>DB FreeGB proj.</th>"
$allDbsTable += "<th>LogVol</th><th>Log Free% vorher</th><th>Log FreeGB vorher</th><th>LogNeed (GB)</th><th>Log Free% proj.</th><th>Log FreeGB proj.</th>"
$allDbsTable += "</tr></thead><tbody>"

# Sort-Object mit Hashtables 
$sortedAllDbRows = $allDbRows | Sort-Object -Property `
    @{ Expression = 'ReasonOrder';  Ascending = $true  }, `
    @{ Expression = { if ($_.IsTarget -eq 'YES') { 1 } else { 0 } }; Ascending = $false }, `
    @{ Expression = 'DbFreePctProj'; Ascending = $false }, `
    @{ Expression = 'DB';           Ascending = $true  }

foreach ($r in $sortedAllDbRows) {
    $allDbsTable += "<tr>" +
        "<td><b>$($r.DB)</b></td>" +
        "<td>$($r.IsTarget)</td>" +
        "<td>$($r.Reason)</td>" +
        "<td>$($r.DBVol)</td>" +
        "<td>$($r.DbFreePctBefore)%</td>" +
        "<td>$($r.DbFreeGBBefore)</td>" +
        "<td>$($r.WhitespaceGB)</td>" +
        "<td><b>$($r.NeedGrowthGB)</b></td>" +
        "<td><b>$($r.DbFreePctProj)%</b></td>" +
        "<td><b>$($r.DbFreeGBProj)</b></td>" +
        "<td>$($r.LogVol)</td>" +
        "<td>$($r.LogFreePctBefore)%</td>" +
        "<td>$($r.LogFreeGBBefore)</td>" +
        "<td><b>$($r.LogNeedGB)</b></td>" +
        "<td><b>$($r.LogFreePctProj)%</b></td>" +
        "<td><b>$($r.LogFreeGBProj)</b></td>" +
        "</tr>"
}
$allDbsTable += "</tbody></table>"
$allDbsTableHtml = ($allDbsTable -join "`n")

# Source Impact (EXTRA TABLE)
$sourceImpactTable = @()
$sourceImpactTable += "<h2>Source Impact (Entlastung / Whitespace-Zuwachs)</h2>"
$sourceImpactTable += "<p class='small'>Moves von Source-DBs erzeugen typischerweise <b>Whitespace</b>, aber nicht automatisch mehr <b>Volume-Free%</b>, weil die EDB-Datei i.d.R. nicht von selbst schrumpft.</p>"
$sourceImpactTable += "<table><thead><tr>"
$sourceImpactTable += "<th>Source DB</th><th>Moves Out</th><th>MoveOut (GB)</th><th>Expected Whitespace Gain (GB)</th>"
$sourceImpactTable += "</tr></thead><tbody>"

foreach ($r in $sourceImpactRows) {
    $sourceImpactTable += "<tr>" +
        "<td><b>$($r.SourceDB)</b></td>" +
        "<td>$($r.MovesOut)</td>" +
        "<td><b>$($r.MoveOutGB)</b></td>" +
        "<td><b>$($r.ExpectedWhitespaceGainGB)</b></td>" +
        "</tr>"
}
$sourceImpactTable += "</tbody></table>"
$sourceImpactHtml = ($sourceImpactTable -join "`n")

# Plan tables
$planHtml = $plan |
    Select-Object DisplayName, SourceDatabase, TargetDatabase, SizeMB |
    Sort-Object TargetDatabase, SizeMB -Descending |
    ConvertTo-Html -Fragment -PreContent "<h2>Plan (kompakt)</h2>"

$fullPlanHtml = $plan |
    Sort-Object TargetDatabase, SizeMB -Descending |
    ConvertTo-Html -Fragment -PreContent "<h2>Plan (vollständig)</h2>"

$style = @"
<style>
body{font-family:Segoe UI,Arial,sans-serif;margin:24px;}
h1{margin-bottom:6px;}
h2{margin-top:24px;}
.small{color:#444;font-size:13px;line-height:1.35;}
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
th,td{border:1px solid #ddd;padding:8px;font-size:13px;vertical-align:top;}
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
<b>MinFreePercent (DB):</b> $MinFreePercent<br/>
<b>MinLogFreePercent (LOG):</b> $MinLogFreePercent<br/>
<b>LogGrowthFactorGBPerMovedGB:</b> $LogGrowthFactorGBPerMovedGB<br/>
<b>Batch:</b> Size=$BatchSize / Number=$BatchNumber / MaxTotalMoveGB=$MaxTotalMoveGB<br/>
<b>Targets:</b> $($targetDBs -join ', ')
</p>

$top10Html
$summaryTableHtml
$allDbsTableHtml
$sourceImpactHtml
$planHtml
$fullPlanHtml
</body></html>
"@ | Out-File -Encoding UTF8 -FilePath $htmlPath

Write-Host "`nReport geschrieben:" -ForegroundColor Cyan
Write-Host "  CSV : $csvPath"
Write-Host "  HTML: $htmlPath"

if ($WhatIf) {
    try { Invoke-Item -Path $htmlPath } catch { Write-Warning "Konnte HTML-Report nicht automatisch öffnen: $($_.Exception.Message)" }
}

# -----------------------------
# Optional: MoveRequests erstellen
# -----------------------------

Write-Host "`nMoveRequests:" -ForegroundColor Cyan

foreach ($db in $targetDBs) {
    foreach ($m in $distributions[$db]) {
        $msg = "Move '$($m.DisplayName)' nach $db ($($m.SizeMB) MB)"

        if ($WhatIf) {
            Write-Host "[WhatIf] $msg"
            continue
        }

        $existing = $null
        try { $existing = Get-MoveRequest -Identity $m.Identity -ErrorAction SilentlyContinue } catch { $existing = $null }

        if ($existing) {
            Write-Warning "Überspringe '$($m.DisplayName)': MoveRequest existiert bereits (Status: $($existing.Status))"
            continue
        }

        Write-Host "Starte $msg"
        New-MoveRequest -Identity $m.Identity -TargetDatabase $db -SuspendWhenReadyToComplete
    }
}

Write-Host "`nDone." -ForegroundColor Green
