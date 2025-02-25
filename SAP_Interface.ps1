# SAP_Interface.ps1 - Script for SAP data integration
# ----------------------------------------------------

# Set error behavior
$ErrorActionPreference = "Stop"

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
$ftpServer = $config.FtpServer
$ftpPath = $config.FtpPath
$ftpUser = $config.FtpUser
$ftpPassword = $config.FtpPassword
$interfacePrefix = $config.InterfacePrefix

# Define directory paths relative to script location
$outputFolder = Join-Path -Path $PSScriptRoot -ChildPath "Files"
$logPath = Join-Path -Path $PSScriptRoot -ChildPath "Logs"

# Create timestamp for file naming
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFileName = "SAP_Interface_Log_$timestamp.txt"
$logFile = Join-Path -Path $logPath -ChildPath $logFileName

# Set up logging function
function Write-Log {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Message,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("INFO", "WARNING", "ERROR")]
        [string]$LogLevel = "INFO"
    )
    
    $logEntry = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - [$LogLevel] - $Message"
    
    # Output to console
    if ($LogLevel -eq "ERROR") {
        Write-Host $logEntry -ForegroundColor Red
    } elseif ($LogLevel -eq "WARNING") {
        Write-Host $logEntry -ForegroundColor Yellow
    } else {
        Write-Host $logEntry
    }
    
    # Write to log file if available
    if ($script:logFile) {
        try {
            Add-Content -Path $script:logFile -Value $logEntry -ErrorAction SilentlyContinue
        } catch {
            # Fail silently if unable to write to log
        }
    }
}

# Main script execution
try {
    Write-Log "Starting SAP interface process..."
    
    # Create output directory if it doesn't exist
    try {
        if (!(Test-Path -LiteralPath $outputFolder)) {
            New-Item -Path $outputFolder -ItemType Directory -Force | Out-Null
            Write-Log "Created output directory: $outputFolder"
        }
    } catch {
        Write-Log "ERROR: Failed to create output directory: $_" -LogLevel "ERROR"
        throw
    }
    
    # Create log directory if it doesn't exist
    try {
        if (!(Test-Path -LiteralPath $logPath)) {
            New-Item -Path $logPath -ItemType Directory -Force | Out-Null
            Write-Log "Created log directory: $logPath"
        }
    } catch {
        Write-Log "ERROR: Failed to create log directory: $_" -LogLevel "ERROR"
        throw
    }
    
    # Set up file names
    $outputFileName = "${interfacePrefix}_${timestamp}_0001.txt"
    $outputFilePath = Join-Path -Path $outputFolder -ChildPath $outputFileName
    $okFilePath = "$outputFilePath.OK"
    
    Write-Log "Output file will be: $outputFileName"
    
    # Connect to database
    try {
        $connectionString = "Server=$sqlServer;Database=$database;User Id=$sqlUser;Password=$sqlPassword;"
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $connection.Open()
        
        if ($connection.State -ne 'Open') {
            throw "Failed to open database connection"
        }
        
        Write-Log "Database connection established"
    } catch {
        Write-Log "ERROR: Database connection failed: $_" -LogLevel "ERROR"
        throw
    }
    
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
    
    # Execute query
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
    
    # Validate the generated file
    if (!(Test-Path -LiteralPath $outputFilePath)) {
        throw "Output file was not created: $outputFilePath"
    }
    
    $fileSize = (Get-Item -LiteralPath $outputFilePath).Length
    if ($fileSize -eq 0) {
        throw "Output file is empty: $outputFilePath"
    }
    
    # Create the OK file
    New-Item -Path $okFilePath -ItemType File -Force | Out-Null
    Write-Log "Created OK marker file"
    
    # Upload files to FTP
    Write-Log "Initiating FTP upload..."
    
    # Function for FTP upload with retry logic
    function Upload-FileToFtp {
        param (
            [string]$LocalPath,
            [string]$RemoteUrl,
            [int]$MaxRetries = 3,
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
                    throw
                }
                
                Write-Log "WARNING: Upload attempt $retryCount failed. Retrying in $WaitSeconds seconds..." -LogLevel "WARNING"
                Start-Sleep -Seconds $WaitSeconds
            }
        } while ($retryCount -lt $MaxRetries)
        
        return $success
    }
    
    # Upload main file and OK file
    $ftpUploadUrl = "ftp://$ftpServer$ftpPath$outputFileName"
    Upload-FileToFtp -LocalPath $outputFilePath -RemoteUrl $ftpUploadUrl
    
    $ftpUploadUrlOK = "ftp://$ftpServer$ftpPath$outputFileName.OK"
    Upload-FileToFtp -LocalPath $okFilePath -RemoteUrl $ftpUploadUrlOK
    
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
    $updateCommand.Parameters.AddWithValue("@fileName", $outputFileName) | Out-Null
    $recordsUpdated = $updateCommand.ExecuteNonQuery()
    
    Write-Log "Updated $recordsUpdated records in database to status 'SENT'"
    Write-Log "Interface process completed successfully"
} 
catch {
    Write-Log "ERROR: Process failed: $($_.Exception.Message)" -LogLevel "ERROR"
    Write-Log "Stack trace: $($_.ScriptStackTrace)" -LogLevel "ERROR"
    
    # Exit with error
    exit 1
}
finally {
    # Clean up resources
    if ($null -ne $streamWriter) {
        $streamWriter.Dispose()
    }
    
    if ($null -ne $reader -and -not $reader.IsClosed) {
        $reader.Close()
    }
    
    if ($null -ne $connection -and $connection.State -eq 'Open') {
        $connection.Close()
        Write-Log "Database connection closed"
    }
}

Write-Log "Script execution complete"
