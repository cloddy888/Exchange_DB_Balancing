# Exchange_DB_balancing

This PowerShell script collects all user mailboxes from the Exchange organization, calculates the total size (MB) for each mailbox, and then distributes the mailboxes in a weighted manner across a defined list of target mailbox databases (DB01–DB04).

The assignment is based on a simple load score to ensure that no single DB “takes on everything”:

LoadScore = (Number of mailboxes in DB × 10) + (Total MB in DB)

For each mailbox, the script always selects the database that currently has the lowest load score.
At the end, the script displays a preview of the distribution and can optionally create MoveRequests.

Special feature: The MoveRequests are created with -SuspendWhenReadyToComplete—meaning they run until “just before completion” and then wait for your approval for the final cutover. (Very useful for maintenance windows, because you can time the final switchover phase.)

*Parameters**

_-WhatIf_ 
Enables simulation mode: No MoveRequests are created; only the planned actions are displayed.

**Prerequisites / Context**

Run in the Exchange Management Shell (or PowerShell with Exchange cmdlets loaded).

Permissions for:

Get-Mailbox, Get-MailboxStatistics

_New-MoveRequest_

The databases in $targetDBs must exist and be accessible.

