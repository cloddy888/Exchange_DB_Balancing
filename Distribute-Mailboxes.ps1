<#
# Distribute-Mailboxes.ps1

## Synopsis
Dieses Skript sammelt alle **UserMailbox**-Postfächer, ermittelt pro Postfach die **Größe in MB** und verteilt die Postfächer anschließend **gewichtet** auf definierte **Ziel-Mailboxdatenbanken**.

Verteil-Logik (Load-Score):
**LoadScore = (MailboxCount × 10) + (SumSizeMB)**

Für jedes Postfach wird die DB mit dem aktuell kleinsten Load-Score gewählt. Danach wird eine Übersicht je DB ausgegeben und optional werden MoveRequests erstellt.

Hinweis: MoveRequests werden mit `-SuspendWhenReadyToComplete` angelegt, damit der finale Cutover gezielt im Wartungsfenster erfolgen kann.

## Parameter
- `-WhatIf`  
  Simulation: Keine MoveRequests, nur Ausgabe der geplanten Aktionen.

## Beispiele
Planung (Simulation):
    .\Distribute-Mailboxes.ps1 -WhatIf

Ausführung (MoveRequests erstellen):
    .\Distribute-Mailboxes.ps1

Move-Status prüfen:
    Get-MoveRequest | Get-MoveRequestStatistics |
      Select DisplayName, Status, PercentComplete, TotalMailboxSize, TargetDatabase

Finalisierung im Wartungsfenster:
    Get-MoveRequest -MoveStatus Suspended | Resume-MoveRequest
#>


param(
    [switch]$WhatIf
)

# === Ziel-Datenbanken festlegen ===
$targetDBs = @("DB01", "DB02", "DB03", "DB04")

# === Mailboxen einsammeln (robust) ===
$mailboxes = Get-Mailbox -ResultSize Unlimited |
  Where-Object { $_.RecipientTypeDetails -eq 'UserMailbox' } |
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
      $sizeMB = [math]::Round($stats.TotalItemSize.Value.ToMB(), 2)
    }

    [PSCustomObject]@{
      DisplayName = $mbx.DisplayName
      Identity    = $mbx.Identity
      SizeMB      = $sizeMB
    }
  }

# === Initialisiere Verteilungsstruktur ===
$distributions = @{}
foreach ($db in $targetDBs) {
    $distributions[$db] = @()
}

# === Gewichtete Zuweisung nach "Load Score" ===
foreach ($mb in $mailboxes) {
    # LoadScore = Anzahl*10 + SummeMB
    $targetDB = ($distributions.GetEnumerator() | Sort-Object {
        ($_.Value.Count * 10) + ($_.Value | Measure-Object SizeMB -Sum).Sum
    })[0].Key

    $distributions[$targetDB] += $mb
}

# === Ausgabe: Vorschau-Verteilung ===
$distributions.GetEnumerator() | ForEach-Object {
    $count = $_.Value.Count
    $sum   = ($_.Value | Measure-Object SizeMB -Sum).Sum
    Write-Host "`n==> $($_.Key): $count Postfächer, Gesamtgröße: $([math]::Round($sum,2)) MB"
}

# === Optional: MoveRequests erstellen ===
foreach ($db in $targetDBs) {
    $distributions[$db] | ForEach-Object {
        $msg = "Move '$($_.DisplayName)' nach $db (${($_.SizeMB)} MB)"
        if ($WhatIf) {
            Write-Host "[WhatIf] $msg"
        } else {
            Write-Host "Starte $msg"
            New-MoveRequest -Identity $_.Identity -TargetDatabase $db -SuspendWhenReadyToComplete
        }
    }
}