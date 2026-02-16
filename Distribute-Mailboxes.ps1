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

    $vol = Get-VolumeInfoForPath -Path $edbPath

    $dbInfo[$dbName] = [pscustomobject]@{
        DBName     = $dbName
        EdbPath    = $edbPath
        VolLabel   = $vol.Label
        VolName    = $vol.Name
        CapacityGB = $vol.CapacityGB
        FreeGB     = $vol.FreeGB
        FreePct    = $vol.FreePct
    }
}

Write-Host "\nDB/Volume Übersicht (Exchange-DBName ↔ Volume):" -ForegroundColor Cyan
$dbInfo.Values | Sort-Object FreePct | Format-Table DBName, VolLabel, FreePct, FreeGB, CapacityGB, EdbPath -AutoSize

# === Filter DBs by MinFreePercent ===
$targetDBs = $targetDBs | Where-Object { $dbInfo[$_].FreePct -ge $MinFreePercent }

if (-not $targetDBs -or $targetDBs.Count -lt 1) {
    throw "Nach MinFreePercent=$MinFreePercent bleibt keine Ziel-DB übrig."
}

Write-Host "\nZiel-DBs nach Filter (MinFreePercent=$MinFreePercent): $($targetDBs -join ', ')" -ForegroundColor Cyan

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

# === Optional: MoveRequests erstellen ===
Write-Host "\nMoveRequests:" -ForegroundColor Cyan

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

Write-Host "\nDone." -ForegroundColor Green
