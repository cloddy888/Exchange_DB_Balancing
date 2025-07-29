param(
    [switch]$WhatIf
)

# === Ziel-Datenbanken festlegen ===
$targetDBs = @("DB01", "DB02", "DB03", "DB04")

# === Mailboxen einsammeln ===
$mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object { $_.RecipientTypeDetails -eq 'UserMailbox' } |
    ForEach-Object {
        $stats = Get-MailboxStatistics $_.Identity
        [PSCustomObject]@{
            DisplayName = $_.DisplayName
            Identity    = $_.Identity
            SizeMB      = [math]::Round($stats.TotalItemSize.Value.ToMB(), 2)
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
