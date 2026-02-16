# Exchange_DB_balancing

Dieses PowerShell-Skript sammelt alle UserMailbox-Postfächer aus der Exchange-Organisation, ermittelt pro Postfach die Gesamtgröße (MB) und verteilt die Postfächer anschließend gewichtet auf eine definierte Liste von Ziel-Mailboxdatenbanken (DB01–DB04).

Die Zuweisung passiert nach einem einfachen Load-Score, damit nicht eine DB „alles abbekommt“:

LoadScore = (Anzahl Postfächer in DB × 10) + (Summe der MB in DB)

Für jedes Postfach wird immer die Datenbank gewählt, die aktuell den kleinsten Load-Score hat.
Am Ende gibt das Skript eine Verteilungs-Vorschau aus und kann optional MoveRequests erstellen.

Besonderheit: Die MoveRequests werden mit -SuspendWhenReadyToComplete angelegt – sie laufen also bis „kurz vor fertig“ und warten dann auf deine Freigabe zum finalen Cutover. (Sehr nett für Wartungsfenster, weil du die letzte Umschaltphase timen kannst.)

**Parameter**

_-WhatIf_ 
Schaltet in den Simulationsmodus: Es werden keine MoveRequests erstellt, sondern nur die geplanten Aktionen ausgegeben.

**Voraussetzungen / Kontext**

Ausführung in der Exchange Management Shell (oder PowerShell mit geladenen Exchange-Cmdlets).

Berechtigungen für:

Get-Mailbox, Get-MailboxStatistics

_New-MoveRequest_

Die Datenbanken in $targetDBs müssen existieren und erreichbar sein.