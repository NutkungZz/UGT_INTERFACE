# SAP_Interface.ps1 - Script for SAP data integration with enhanced path handling
# -----------------------------------------------------------------------------

# Set error behavior
$ErrorActionPreference = "Stop"

# Ensure required .NET types are available
Add-Type -AssemblyName System.IO
Add-Type -AssemblyName System.Data

# Get script location for relative paths
$scriptLocation = [System.IO.Path]::GetDirectoryName($MyInvocation.MyCommand.Definition)

# Load configuration from file
$configFileName = "SAP_Interface_Config.ini"
$configPath = [System.IO.Path]::Combine($scriptLocation, $configFileName)

if (-not [System.IO.File]::Exists($configPath)) {
    Write-Host "Configuration file not found: $configPath" -ForegroundColor Red
    exit 1
}

# Parse configuration safely
$config = @{}
foreach ($line in [System.IO.File]::ReadAllLines($configPath)) {
    $line = $line.Trim()
    if ($line -and -not $line.StartsWith('#')) {
        $parts = $line -split '=', 2
        if ($parts.Count -eq 2) {
            $key = $parts[0].Trim()
            $value = $parts[1].Trim()
            # Remove quotes if present
            if ($value.StartsWith('"') -and $value.EndsWith('"')) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            $config[$key] = $value
        }
    }
}

# Set configuration variables
$sqlServer = $config["SqlServer"]
$database = $config["Database"]
$sqlUser = $config["SqlUser"]
$sqlPassword = $config["SqlPassword"]
$ftpServer = $config["FtpServer"]
$ftpPath = $config["FtpPath"]
$ftpUser = $config["FtpUser"]
$ftpPassword = $config["FtpPassword"]
$interfacePrefix = if ($config.ContainsKey("InterfacePrefix")) { $config["InterfacePrefix"] } else { "ZCSE086_OPERAND2" }

# Define directory paths using .NET methods
$filesDir = [System.IO.Path]::Combine($scriptLocation, "Files")
$logsDir = [System.IO.Path]::Combine($scriptLocation, "Logs")

# Create timestamp for file naming
$timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$logFileName = "SAP_Interface_Log_$timestamp.txt"
$logFile = [System.IO.Path]::Combine($logsDir, $logFileName)

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
    
    # Output to console with appropriate color
    if ($LogLevel -eq "ERROR") {
        Write-Host $logEntry -ForegroundColor Red
    } elseif ($LogLevel -eq "WARNING") {
        Write-Host $logEntry -ForegroundColor Yellow
    } else {
        Write-Host $logEntry
    }
    
    # Write to log file if possible
    try {
        if (-not [string]::IsNullOrEmpty($script:logFile) -and [System.IO.Directory]::Exists([System.IO.Path]::GetDirectoryName($script:logFile))) {
            [System.IO.File]::AppendAllText($script:logFile, $logEntry + [Environment]::NewLine)
        }
    } catch {
        # Silent fail on logging errors
    }
}

# Main script execution
try {
    Write-Log "Starting SAP interface process..."
    
    # Create output directory if it doesn't exist
    try {
        if (-not [System.IO.Directory]::Exists($filesDir)) {
            [System.IO.Directory]::CreateDirectory($filesDir)
            Write-Log "Created output directory: $filesDir"
        }
    } catch {
        Write-Log "ERROR: Failed to create output directory: $_" -LogLevel "ERROR"
        throw
    }
    
    # Create log directory if it doesn't exist
    try {
        if (-not [System.IO.Directory]::Exists($logsDir)) {
            [System.IO.Directory]::CreateDirectory($logsDir)
            Write-Log "Created log directory: $logsDir"
        }
    } catch {
        Write-Log "ERROR: Failed to create log directory: $_" -LogLevel "ERROR"
        throw
    }
    
    # Set up file names using .NET path methods
    $baseFileName = "$interfacePrefix`_$timestamp`_0001"
    $outputFileName = "$baseFileName.txt"
    $outputFilePath = [System.IO.Path]::Combine($filesDir, $outputFileName)
    $okFilePath = [System.IO.Path]::Combine($filesDir, "$baseFileName.OK")
    
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
    $command = New-Object System.Data.SqlClient.SqlCommand($query, $connection)
    $reader = $command.ExecuteReader()
    
    # Check if we have records to process
    if (-not $reader.HasRows) {
        Write-Log "No pending records found for processing"
        $reader.Close()
        $connection.Close()
        exit 0
    }
    
    # Create the interface file using .NET methods
    $fileStream = New-Object System.IO.FileStream($outputFilePath, [System.IO.FileMode]::Create)
    $streamWriter = New-Object System.IO.StreamWriter($fileStream, [System.Text.Encoding]::UTF8)
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
    
    # Close file handles properly
    $streamWriter.Flush()
    $streamWriter.Close()
    $fileStream.Close()
    $reader.Close()
    
    Write-Log "Generated interface file with $recordCount records"
    
    # Validate the generated file
    if (-not [System.IO.File]::Exists($outputFilePath)) {
        throw "Output file was not created: $outputFilePath"
    }
    
    $fileSize = (New-Object System.IO.FileInfo($outputFilePath)).Length
    if ($fileSize -eq 0) {
        throw "Output file is empty: $outputFilePath"
    }
    
    # Create the OK file using .NET methods
    [System.IO.File]::WriteAllText($okFilePath, "")
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
    
    $updateCommand = New-Object System.Data.SqlClient.SqlCommand($updateQuery, $connection)
    $updateCommand.Parameters.AddWithValue("@fileName", $outputFileName) | Out-Null
    $recordsUpdated = $updateCommand.ExecuteNonQuery()
    
    Write-Log "Updated $recordsUpdated records in database to status 'SENT'"
    Write-Log "Interface process completed successfully"
} 
catch {
    Write-Log "ERROR: Process failed: $($_.Exception.Message)" -LogLevel "ERROR"
    if ($_.ScriptStackTrace) {
        Write-Log "Stack trace: $($_.ScriptStackTrace)" -LogLevel "ERROR"
    }
    
    # Exit with error
    exit 1
}
finally {
    # Clean up resources
    if ($null -ne $streamWriter -and $streamWriter -is [System.IDisposable]) {
        try { $streamWriter.Dispose() } catch {}
    }
    
    if ($null -ne $fileStream -and $fileStream -is [System.IDisposable]) {
        try { $fileStream.Dispose() } catch {}
    }
    
    if ($null -ne $reader -and -not $reader.IsClosed) {
        try { $reader.Close() } catch {}
    }
    
    if ($null -ne $connection -and $connection.State -eq 'Open') {
        try { 
            $connection.Close() 
            Write-Log "Database connection closed"
        } catch {}
    }
}

Write-Log "Script execution complete"
