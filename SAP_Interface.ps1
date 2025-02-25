<#
.SYNOPSIS
    SAP Integration script to transfer data from SQL Server to SAP via FTP.
.DESCRIPTION
    This script extracts pending data from the INTF_INBOUND table, generates a text file,
    and uploads it to an FTP server for SAP integration. It handles path validation,
    error recovery, and transaction management.
.NOTES
    Version:        2.0
    Author:         Claude
    Creation Date:  2025-02-25
    Last Modified:  2025-02-25
#>

# Enable strict mode for better error detection
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

#region Configuration Loading
Write-Host "Starting SAP Interface process..."
# Load configuration from file
$configPath = "$PSScriptRoot\SAP_Interface_Config.ini"
if (-not (Test-Path -Path $configPath)) {
    Write-Host "Configuration file not found: $configPath"
    exit 1
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
$maxRetries = if ($config.ContainsKey("MaxRetries")) { [int]$config.MaxRetries } else { 3 }
$retryWaitSeconds = if ($config.ContainsKey("RetryWaitSeconds")) { [int]$config.RetryWaitSeconds } else { 5 }
#endregion

#region Function Definitions
# Set up logging
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFileName = "$interfacePrefix`_Log_$timestamp.txt"

function Initialize-Directory {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Path
    )

    try {
        if (-not [string]::IsNullOrWhiteSpace($Path)) {
            if (-not (Test-Path -Path $Path)) {
                New-Item -Path $Path -ItemType Directory -Force | Out-Null
                Write-Log "Created directory: $Path"
            }
            return $true
        } else {
            Write-Log "ERROR: Path is empty or null" -LogLevel "ERROR"
            return $false
        }
    } catch {
        Write-Log "ERROR: Failed to create directory '$Path': $_" -LogLevel "ERROR"
        return $false
    }
}

function Write-Log {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Message,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("INFO", "WARNING", "ERROR")]
        [string]$LogLevel = "INFO"
    )
    
    # Ensure log directory exists
    if (-not (Test-Path -Path $logPath)) {
        try {
            New-Item -Path $logPath -ItemType Directory -Force | Out-Null
        } catch {
            # If we can't create the log directory, output to console only
            $consoleMessage = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - [$LogLevel] - $Message"
            Write-Host $consoleMessage
            return
        }
    }
    
    $logFile = Join-Path -Path $logPath -ChildPath $logFileName
    $logEntry = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - [$LogLevel] - $Message"
    
    try {
        Add-Content -Path $logFile -Value $logEntry
    } catch {
        # If we can't write to the log file, at least output to console
        Write-Host "WARNING: Could not write to log file: $_"
    }
    
    # Always write to console as well
    if ($LogLevel -eq "ERROR") {
        Write-Host $logEntry -ForegroundColor Red
    } elseif ($LogLevel -eq "WARNING") {
        Write-Host $logEntry -ForegroundColor Yellow
    } else {
        Write-Host $logEntry
    }
}

function Upload-FileWithRetry {
    param (
        [Parameter(Mandatory=$true)]
        [string]$LocalPath,
        
        [Parameter(Mandatory=$true)]
        [string]$RemoteUrl,
        
        [Parameter(Mandatory=$false)]
        [int]$MaxRetries = 3,
        
        [Parameter(Mandatory=$false)]
        [int]$WaitSeconds = 5
    )
    
    $retryCount = 0
    $success = $false
    
    do {
        try {
            $webClient = New-Object System.Net.WebClient
            $webClient.Credentials = New-Object System.Net.NetworkCredential($ftpUser, $ftpPassword)
            $webClient.UploadFile($RemoteUrl, $LocalPath)
            $success = $true
            Write-Log "Successfully uploaded file to $RemoteUrl"
            break
        } catch {
            $retryCount++
            if ($retryCount -ge $MaxRetries) {
                Write-Log "ERROR: Failed to upload file after $MaxRetries attempts: $_" -LogLevel "ERROR"
                throw $_  # Re-throw to be caught by calling function
            }
            
            Write-Log "WARNING: Upload attempt $retryCount failed. Retrying in $WaitSeconds seconds..." -LogLevel "WARNING"
            Start-Sleep -Seconds $WaitSeconds
        }
    } while ($retryCount -lt $MaxRetries)
    
    return $success
}
#endregion

#region Main Execution
# Validate and initialize directories
$pathsValid = $true
Write-Host "Validating paths..."

$pathsValid = $pathsValid -and (Initialize-Directory -Path $outputFolder)
$pathsValid = $pathsValid -and (Initialize-Directory -Path $logPath)

if (-not $pathsValid) {
    Write-Host "ERROR: One or more required paths could not be created or accessed."
    exit 1
}

# Set up file names
$interfaceTimestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFileName = "${interfacePrefix}_${interfaceTimestamp}_0001.txt"
$outputFilePath = Join-Path -Path $outputFolder -ChildPath $outputFileName
$okFilePath = "$outputFilePath.OK"

Write-Log "Starting SAP interface process for $interfacePrefix"
Write-Log "Output file will be: $outputFileName"

# Main execution block
try {
    # Database connection
    Write-Log "Connecting to database $database on $sqlServer..."
    $connectionString = "Server=$sqlServer;Database=$database;User Id=$sqlUser;Password=$sqlPassword;"
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()
    
    if ($connection.State -ne 'Open') {
        throw "Failed to open database connection. Connection state: $($connection.State)"
    }
    
    Write-Log "Database connection established."
    
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
        exit 0
    }
    
    # Create the interface file
    Write-Log "Generating interface file..."
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
    
    # Validate file
    if (-not (Test-Path -Path $outputFilePath)) {
        throw "Output file was not created: $outputFilePath"
    }
    
    $fileSize = (Get-Item -Path $outputFilePath).Length
    if ($fileSize -eq 0) {
        throw "Output file was created but is empty: $outputFilePath"
    }
    
    Write-Log "Output file validation successful. File size: $fileSize bytes"
    
    # Create the OK file
    New-Item -Path $okFilePath -ItemType File -Force | Out-Null
    Write-Log "Created OK marker file: $okFilePath"
    
    # Upload files to FTP
    Write-Log "Initiating FTP upload..."
    
    # Upload main file
    $ftpUploadUrl = "ftp://$ftpServer$ftpPath$outputFileName"
    $mainFileUploaded = Upload-FileWithRetry -LocalPath $outputFilePath -RemoteUrl $ftpUploadUrl -MaxRetries $maxRetries -WaitSeconds $retryWaitSeconds
    
    # Upload OK file
    $ftpUploadUrlOK = "ftp://$ftpServer$ftpPath$outputFileName.OK"
    $okFileUploaded = Upload-FileWithRetry -LocalPath $okFilePath -RemoteUrl $ftpUploadUrlOK -MaxRetries $maxRetries -WaitSeconds $retryWaitSeconds
    
    # Update database records to mark as sent
    Write-Log "Updating database records..."
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
    $errorMessage = $_.Exception.Message
    $errorStack = $_.ScriptStackTrace
    Write-Log "ERROR: $errorMessage" -LogLevel "ERROR"
    Write-Log "Stack trace: $errorStack" -LogLevel "ERROR"
    
    # Try to send notification of failure if possible
    try {
        # You can add email notification or other alerting here
        # Example: Send-MailMessage -To "admin@company.com" -Subject "SAP Interface Error" -Body "Error occurred: $errorMessage"
    } catch {
        Write-Log "Failed to send error notification: $_" -LogLevel "ERROR"
    }
    
    # Set non-zero exit code to indicate failure
    exit 1
}
finally {
    # Close resources
    if ($streamWriter -ne $null -and $streamWriter -is [System.IDisposable]) {
        $streamWriter.Dispose()
    }
    
    if ($reader -ne $null -and -not $reader.IsClosed) {
        $reader.Close()
    }
    
    # Close database connection
    if ($connection -ne $null -and $connection.State -eq 'Open') {
        $connection.Close()
        Write-Log "Database connection closed"
    }
}
#endregion
