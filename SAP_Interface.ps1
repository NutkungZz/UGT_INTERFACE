# Load configuration from file
$configPath = "$PSScriptRoot\SAP_Interface_Config.ini"
if (-not (Test-Path -Path $configPath)) {
    Write-Host "Configuration file not found: $configPath"
    exit
}

$config = @{}
Get-Content $configPath | ForEach-Object {
    if ($_ -match '^\s*([^#]\S+)\s*=\s*(.+)') {
        $config[$matches[1].Trim()] = $matches[2].Trim()
    }
}

# Set configuration variables
$sqlServer = $config.SqlServer
$database = $config.Database
$sqlUser = $config.SqlUser
$sqlPassword = $config.SqlPassword
$outputFolder = $config.OutputFolder
$ftpServer = $config.FtpServer
$ftpPath = $config.FtpPath
$ftpUser = $config.FtpUser
$ftpPassword = $config.FtpPassword
$logPath = $config.LogPath
$interfacePrefix = $config.InterfacePrefix

# Create output and log directories if they don't exist
if (-not (Test-Path -Path $outputFolder)) {
    New-Item -Path $outputFolder -ItemType Directory -Force | Out-Null
}
if (-not (Test-Path -Path $logPath)) {
    New-Item -Path $logPath -ItemType Directory -Force | Out-Null
}

# Set up logging
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = "$logPath\ZCSE086_Log_$timestamp.txt"
$ErrorActionPreference = "Stop"

function Write-Log {
    param (
        [string]$Message
    )
    
    $logEntry = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $Message"
    Add-Content -Path $logFile -Value $logEntry
    Write-Host $logEntry
}

# Generate interface file name
$interfaceTimestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFileName = "ZCSE086_OPERAND2_${interfaceTimestamp}_0001.txt"
$outputFilePath = "$outputFolder\$outputFileName"
$okFilePath = "$outputFilePath.OK"

# Connect to SQL Server
try {
    Write-Log "Starting interface file generation process"
    Write-Log "Creating file: $outputFileName"
    
    $connectionString = "Server=$sqlServer;Database=$database;User Id=$sqlUser;Password=$sqlPassword;"
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()
    
    if ($connection.State -ne 'Open') {
        throw "Failed to open database connection"
    }
    
    Write-Log "Database connection established"
    
    # Query for pending records
    $query = @"
SELECT 
    [INSTALLATION], 
    [OPERAND], 
    FORMAT([START_DATE], 'dd.MM.yyyy') AS START_DATE, 
    FORMAT([END_DATE], 'dd.MM.yyyy') AS END_DATE, 
    [ALLOCATE_UNIT], 
    [PERIOD]
FROM 
    [dbo].[INTF_INBOUND]
WHERE 
    [INTERFACE_STATUS] = 'PENDING'
ORDER BY 
    [INSTALLATION], 
    CASE WHEN [OPERAND] = 'UGT1' THEN 1 ELSE 2 END,
    [START_DATE]
"@
    
    $command = $connection.CreateCommand()
    $command.CommandText = $query
    $reader = $command.ExecuteReader()
    
    # Check if we have records to process
    if (-not $reader.HasRows) {
        Write-Log "No pending records found for processing"
        $reader.Close()
        $connection.Close()
        exit
    }
    
    # Create the interface file
    $streamWriter = New-Object System.IO.StreamWriter($outputFilePath, $false, [System.Text.Encoding]::UTF8)
    $recordCount = 0
    
    # Process the records
    while ($reader.Read()) {
        $recordCount++
        $installation = $reader.GetString(0)
        $operand = $reader.GetString(1)
        $startDate = $reader.GetString(2)
        $endDate = $reader.GetString(3)
        $allocateUnit = $reader.GetString(4)
        $period = if ($reader.IsDBNull(5)) { "" } else { $reader.GetString(5) }
        
        # Format the output line with tab separators
        if ($operand -eq 'UGT1') {
            $line = "$installation`t$operand`t$startDate`t$endDate`t$allocateUnit"
        } else {
            $line = "$installation`t$operand`t$startDate`t$endDate`t$allocateUnit`t$period"
        }
        
        $streamWriter.WriteLine($line)
    }
    
    $streamWriter.Close()
    $reader.Close()
    
    Write-Log "Generated interface file with $recordCount records"
    
    # Create the OK file
    New-Item -Path $okFilePath -ItemType File -Force | Out-Null
    Write-Log "Created OK marker file"
    
    # Upload the files to FTP
    try {
        Write-Log "Initiating FTP upload"
        
        $credentials = New-Object System.Net.NetworkCredential($ftpUser, $ftpPassword)
        $webClient = New-Object System.Net.WebClient
        $webClient.Credentials = $credentials
        
        # Upload main file
        $ftpUploadUrl = "ftp://$ftpServer$ftpPath$outputFileName"
        $webClient.UploadFile($ftpUploadUrl, $outputFilePath)
        Write-Log "Uploaded main file to FTP: $ftpUploadUrl"
        
        # Upload OK file
        $ftpUploadUrlOK = "ftp://$ftpServer$ftpPath$outputFileName.OK"
        $webClient.UploadFile($ftpUploadUrlOK, $okFilePath)
        Write-Log "Uploaded OK file to FTP: $ftpUploadUrlOK"
        
        # Update database records to mark as sent
        $updateQuery = @"
UPDATE [dbo].[INTF_INBOUND]
SET 
    [INTERFACE_FILE] = @fileName,
    [INTERFACE_DATE] = GETDATE(),
    [INTERFACE_STATUS] = 'SENT'
WHERE 
    [INTERFACE_STATUS] = 'PENDING'
"@
        
        $updateCommand = $connection.CreateCommand()
        $updateCommand.CommandText = $updateQuery
        $updateParam = $updateCommand.Parameters.AddWithValue("@fileName", $outputFileName)
        $recordsUpdated = $updateCommand.ExecuteNonQuery()
        
        Write-Log "Updated $recordsUpdated records in database to status 'SENT'"
        Write-Log "Interface process completed successfully"
    }
    catch {
        Write-Log "ERROR during FTP upload: $_"
        throw
    }
}
catch {
    Write-Log "ERROR: $_"
    
    if ($streamWriter -ne $null) {
        $streamWriter.Close()
    }
    
    if ($reader -ne $null -and -not $reader.IsClosed) {
        $reader.Close()
    }
}
finally {
    # Close database connection
    if ($connection -ne $null -and $connection.State -eq 'Open') {
        $connection.Close()
        Write-Log "Database connection closed"
    }
}
