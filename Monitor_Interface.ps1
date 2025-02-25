# Load configuration
$configPath = "$PSScriptRoot\SAP_Interface_Config.ini"
$config = @{}
Get-Content $configPath | ForEach-Object {
    if ($_ -match '^\s*([^#]\S+)\s*=\s*(.+)') {
        $config[$matches[1].Trim()] = $matches[2].Trim()
    }
}

# Database connection
$sqlServer = $config.SqlServer
$database = $config.Database
$sqlUser = $config.SqlUser
$sqlPassword = $config.SqlPassword
$connectionString = "Server=$sqlServer;Database=$database;User Id=$sqlUser;Password=$sqlPassword;"

# Connect to database
$connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
$connection.Open()

# Get interface statistics
$query = @"
SELECT 
    [INTERFACE_STATUS],
    COUNT(*) AS RecordCount,
    MAX([INTERFACE_DATE]) AS LastInterfaceDate,
    MIN([INTERFACE_DATE]) AS FirstInterfaceDate
FROM 
    [dbo].[INTF_INBOUND]
GROUP BY 
    [INTERFACE_STATUS]
ORDER BY 
    [INTERFACE_STATUS]
"@

$command = $connection.CreateCommand()
$command.CommandText = $query
$reader = $command.ExecuteReader()

# Display interface statistics
Write-Host "===== SAP Interface Status Report ====="
Write-Host "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`n"

while ($reader.Read()) {
    $status = $reader.IsDBNull(0) ? "NULL" : $reader.GetString(0)
    $count = $reader.GetInt32(1)
    $lastDate = $reader.IsDBNull(2) ? "N/A" : $reader.GetDateTime(2).ToString("yyyy-MM-dd HH:mm:ss")
    $firstDate = $reader.IsDBNull(3) ? "N/A" : $reader.GetDateTime(3).ToString("yyyy-MM-dd HH:mm:ss")
    
    Write-Host "Status: $status"
    Write-Host "Record Count: $count"
    Write-Host "First Interface Date: $firstDate"
    Write-Host "Last Interface Date: $lastDate"
    Write-Host "-------------------------------------"
}

$reader.Close()

# Get recent interface files
$query = @"
SELECT DISTINCT TOP 5
    [INTERFACE_FILE],
    [INTERFACE_DATE],
    COUNT(*) OVER (PARTITION BY [INTERFACE_FILE]) AS RecordCount
FROM 
    [dbo].[INTF_INBOUND]
WHERE 
    [INTERFACE_FILE] IS NOT NULL
ORDER BY 
    [INTERFACE_DATE] DESC
"@

$command = $connection.CreateCommand()
$command.CommandText = $query
$reader = $command.ExecuteReader()

# Display recent files
Write-Host "`n===== Recent Interface Files ====="

while ($reader.Read()) {
    $file = $reader.GetString(0)
    $date = $reader.GetDateTime(1).ToString("yyyy-MM-dd HH:mm:ss")
    $count = $reader.GetInt32(2)
    
    Write-Host "File: $file"
    Write-Host "Date: $date"
    Write-Host "Records: $count"
    Write-Host "-------------------------------------"
}

$reader.Close()
$connection.Close()
