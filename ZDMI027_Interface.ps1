# ZDMI027_FTP_Import.ps1
# Script for importing SAP ZDMI027 data from FTP server to database
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

# Parse configuration
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
$ftpUser = $config["FtpUser"]
$ftpPassword = $config["FtpPassword"]
$ftpPath = "/interface/3rdparty/UGT1/Outbound/ZDMI027/"

# Define directory paths - using relative paths from config if available
$filesDir = if ($config.ContainsKey("OutputFolder")) { 
    [System.IO.Path]::Combine($scriptLocation, $config["OutputFolder"].TrimStart('.\')) 
} else { 
    [System.IO.Path]::Combine($scriptLocation, "Files") 
}

$logsDir = if ($config.ContainsKey("LogPath")) { 
    [System.IO.Path]::Combine($scriptLocation, $config["LogPath"].TrimStart('.\')) 
} else { 
    [System.IO.Path]::Combine($scriptLocation, "Logs") 
}

$archiveDir = [System.IO.Path]::Combine($scriptLocation, "Archive")

# Create timestamp for file naming
$timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$logFileName = "ZDMI027_Import_Log_$timestamp.txt"
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

# Function to parse date strings from SAP format to SQL format
function Convert-SapDateToSql {
    param (
        [string]$SapDate
    )
    
    try {
        if ($SapDate -match '(\d{2})\.(\d{2})\.(\d{4})') {
            $day = $matches[1]
            $month = $matches[2]
            $year = $matches[3]
            return "$year-$month-$day"
        } else {
            throw "Invalid date format: $SapDate"
        }
    } catch {
        Write-Log ("ERROR: Failed to convert date: " + $SapDate + " - " + ${_}) -LogLevel "ERROR"
        throw
    }
}

# Function to check if a file has already been processed
function Test-FileProcessed {
    param (
        [string]$FileName,
        [System.Data.SqlClient.SqlConnection]$Connection
    )
    
    try {
        $query = "SELECT COUNT(*) FROM [dbo].[ZDMI027_OUTBOUND] WHERE [FILE_NAME] = @fileName"
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $command.Parameters.AddWithValue("@fileName", $FileName) | Out-Null
        $count = $command.ExecuteScalar()
        
        return ($count -gt 0)
    } catch {
        Write-Log ("ERROR: Failed to check if file was processed: " + ${_}) -LogLevel "ERROR"
        throw
    }
}

# Function to list files from FTP directory
function Get-FtpDirectory {
    param (
        [string]$FtpPath
    )
    
    try {
        $ftpUrl = "ftp://$ftpServer$FtpPath"
        $request = [System.Net.FtpWebRequest]::Create($ftpUrl)
        $request.Method = [System.Net.WebRequestMethods+Ftp]::ListDirectory
        
        # Check if FTP credentials are provided
        if ([string]::IsNullOrEmpty($ftpUser) -or [string]::IsNullOrEmpty($ftpPassword)) {
            Write-Log "Using anonymous FTP login for directory listing" -LogLevel "INFO"
            $request.Credentials = New-Object System.Net.NetworkCredential("anonymous", "anonymous@domain.com")
        } else {
            $request.Credentials = New-Object System.Net.NetworkCredential($ftpUser, $ftpPassword)
        }
        
        $request.UsePassive = $true
        $request.UseBinary = $true
        $request.KeepAlive = $false
        
        $response = $request.GetResponse()
        $reader = New-Object System.IO.StreamReader($response.GetResponseStream())
        $fileList = $reader.ReadToEnd().Split("`n", [StringSplitOptions]::RemoveEmptyEntries) | 
                    Where-Object { $_ -ne "." -and $_ -ne ".." } |
                    ForEach-Object { $_.Trim() }
        
        $reader.Close()
        $response.Close()
        
        return $fileList
    } catch {
        Write-Log ("ERROR: Failed to list FTP directory: " + ${_}) -LogLevel "ERROR"
        throw
    }
}

# Function to download a file from FTP with retry logic
function Get-FtpFile {
    param (
        [string]$RemoteFile,
        [string]$LocalFile,
        [int]$MaxRetries = 3,
        [int]$RetryWaitSeconds = 5
    )
    
    # Use configured retry parameters if available
    $maxRetryAttempts = if ($config.ContainsKey("MaxRetries")) { [int]$config["MaxRetries"] } else { $MaxRetries }
    $waitSeconds = if ($config.ContainsKey("RetryWaitSeconds")) { [int]$config["RetryWaitSeconds"] } else { $RetryWaitSeconds }
    
    $attempt = 0
    $success = $false
    
    do {
        $attempt++
        try {
            Write-Log "Downloading file $RemoteFile (Attempt $attempt of $maxRetryAttempts)"
            
            $ftpUrl = "ftp://$ftpServer$ftpPath$RemoteFile"
            $request = [System.Net.FtpWebRequest]::Create($ftpUrl)
            $request.Method = [System.Net.WebRequestMethods+Ftp]::DownloadFile
            
            # Check if FTP credentials are provided
            if ([string]::IsNullOrEmpty($ftpUser) -or [string]::IsNullOrEmpty($ftpPassword)) {
                Write-Log "WARNING: FTP credentials not provided in config file. Using anonymous login." -LogLevel "WARNING"
                $request.Credentials = New-Object System.Net.NetworkCredential("anonymous", "anonymous@domain.com")
            } else {
                $request.Credentials = New-Object System.Net.NetworkCredential($ftpUser, $ftpPassword)
            }
            
            $request.UsePassive = $true
            $request.UseBinary = $true
            $request.KeepAlive = $false
            
            $response = $request.GetResponse()
            $responseStream = $response.GetResponseStream()
            $fileStream = [System.IO.File]::Create($LocalFile)
            $buffer = New-Object byte[] 10240
            $bytesRead = 0
            
            do {
                $bytesRead = $responseStream.Read($buffer, 0, $buffer.Length)
                $fileStream.Write($buffer, 0, $bytesRead)
            } while ($bytesRead -ne 0)
            
            $fileStream.Close()
            $responseStream.Close()
            $response.Close()
            
            $success = $true
            Write-Log "Successfully downloaded file $RemoteFile"
            break
        } catch {
            if ($attempt -ge $maxRetryAttempts) {
                Write-Log ("ERROR: Failed to download file " + $RemoteFile + " after " + $maxRetryAttempts + " attempts: " + ${_}) -LogLevel "ERROR"
                throw
            } else {
                Write-Log ("WARNING: Download attempt " + $attempt + " failed. Retrying in " + $waitSeconds + " seconds...") -LogLevel "WARNING"
                Start-Sleep -Seconds $waitSeconds
            }
        }
    } while ($attempt -lt $maxRetryAttempts)
    
    return $success
}

# Function to process the ZDMI027 text file
function Process-Zdmi027File {
    param (
        [string]$FilePath,
        [string]$FileName,
        [System.Data.SqlClient.SqlConnection]$Connection
    )
    
    try {
        $fileContent = [System.IO.File]::ReadAllLines($FilePath)
        $recordCount = 0
        
        # Begin a transaction
        $transaction = $Connection.BeginTransaction()
        
        foreach ($line in $fileContent) {
            if (-not [string]::IsNullOrWhiteSpace($line)) {
                $fields = $line.Split("`t")
                
                # Validate field count
                if ($fields.Length -lt 7) {
                    throw "Invalid record format. Expected at least 7 fields, got $($fields.Length): $line"
                }
                
                $billPeriod = $fields[0].Trim()
                $ca = $fields[1].Trim()
                $installation = $fields[2].Trim()
                $trsg = $fields[3].Trim()
                $ba = $fields[4].Trim()
                $readingDate = Convert-SapDateToSql -SapDate $fields[5].Trim()
                $unitUsed = [double]::Parse($fields[6].Trim())
                
                # Insert into database
                $query = @"
INSERT INTO [dbo].[ZDMI027_OUTBOUND]
    ([BILL_PERIOD], [CA], [INSTALLATION], [TRSG], [BA], [READING_DATE], [UNIT_USED], [FILE_NAME])
VALUES
    (@billPeriod, @ca, @installation, @trsg, @ba, @readingDate, @unitUsed, @fileName)
"@
                
                $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection, $transaction)
                $command.Parameters.AddWithValue("@billPeriod", $billPeriod) | Out-Null
                $command.Parameters.AddWithValue("@ca", $ca) | Out-Null
                $command.Parameters.AddWithValue("@installation", $installation) | Out-Null
                $command.Parameters.AddWithValue("@trsg", $trsg) | Out-Null
                $command.Parameters.AddWithValue("@ba", $ba) | Out-Null
                $command.Parameters.AddWithValue("@readingDate", $readingDate) | Out-Null
                $command.Parameters.AddWithValue("@unitUsed", $unitUsed) | Out-Null
                $command.Parameters.AddWithValue("@fileName", $FileName) | Out-Null
                
                $command.ExecuteNonQuery() | Out-Null
                $recordCount++
            }
        }
        
        # Commit the transaction
        $transaction.Commit()
        
        return $recordCount
    } catch {
        # Roll back the transaction if an error occurs
        if ($null -ne $transaction) {
            try { $transaction.Rollback() } catch {}
        }
        
        Write-Log ("ERROR: Failed to process file " + $FileName + ": " + ${_}) -LogLevel "ERROR"
        throw
    }
}

# Main script execution
try {
    Write-Log "Starting ZDMI027 import process..."
    
    # Create required directories if they don't exist
    foreach ($dir in @($filesDir, $logsDir, $archiveDir)) {
        if (-not [System.IO.Directory]::Exists($dir)) {
            [System.IO.Directory]::CreateDirectory($dir)
            Write-Log "Created directory: $dir"
        }
    }
    
    # Check if Complete folder exists on FTP, if not create it
    try {
        $ftpUrl = "ftp://$ftpServer$ftpPath/Complete"
        $request = [System.Net.FtpWebRequest]::Create($ftpUrl)
        $request.Method = [System.Net.WebRequestMethods+Ftp]::ListDirectory
        
        if ([string]::IsNullOrEmpty($ftpUser) -or [string]::IsNullOrEmpty($ftpPassword)) {
            $request.Credentials = New-Object System.Net.NetworkCredential("anonymous", "anonymous@domain.com")
        } else {
            $request.Credentials = New-Object System.Net.NetworkCredential($ftpUser, $ftpPassword)
        }
        
        $request.UsePassive = $true
        $request.UseBinary = $true
        $request.KeepAlive = $false
        
        try {
            $response = $request.GetResponse()
            $response.Close()
            Write-Log "Complete folder exists on FTP server"
        } catch [System.Net.WebException] {
            # Complete folder doesn't exist, create it
            $makeDirectoryRequest = [System.Net.FtpWebRequest]::Create($ftpUrl)
            $makeDirectoryRequest.Method = [System.Net.WebRequestMethods+Ftp]::MakeDirectory
            
            if ([string]::IsNullOrEmpty($ftpUser) -or [string]::IsNullOrEmpty($ftpPassword)) {
                $makeDirectoryRequest.Credentials = New-Object System.Net.NetworkCredential("anonymous", "anonymous@domain.com")
            } else {
                $makeDirectoryRequest.Credentials = New-Object System.Net.NetworkCredential($ftpUser, $ftpPassword)
            }
            
            $makeDirectoryRequest.UsePassive = $true
            $makeDirectoryRequest.KeepAlive = $false
            
            try {
                $makeDirResponse = $makeDirectoryRequest.GetResponse()
                $makeDirResponse.Close()
                Write-Log "Created Complete folder on FTP server"
            } catch {
                Write-Log ("WARNING: Could not create Complete folder on FTP server: " + ${_}) -LogLevel "WARNING"
                Write-Log "Files will be processed but not moved on the FTP server" -LogLevel "WARNING"
            }
        }
    } catch {
        Write-Log ("WARNING: Error checking/creating Complete folder on FTP: " + ${_}) -LogLevel "WARNING"
    }
    
    # Connect to database
    # Check if SQL credentials are provided, otherwise use Windows Authentication
    if ([string]::IsNullOrEmpty($sqlUser) -or [string]::IsNullOrEmpty($sqlPassword)) {
        $connectionString = "Server=$sqlServer;Database=$database;Integrated Security=True;"
        Write-Log "Using Windows Authentication for database connection"
    } else {
        $connectionString = "Server=$sqlServer;Database=$database;User Id=$sqlUser;Password=$sqlPassword;"
        Write-Log "Using SQL Authentication for database connection"
    }
    
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()
    
    if ($connection.State -ne 'Open') {
        throw "Failed to open database connection"
    }
    
    Write-Log "Database connection established"
    
    # Get list of files from FTP
    $ftpFiles = Get-FtpDirectory -FtpPath $ftpPath
    $dataFiles = $ftpFiles | Where-Object { $_ -like "ZDMI027_G_*.txt" -and $_ -notlike "*.OK" }
    
    Write-Log "Found $($dataFiles.Count) ZDMI027 data files on FTP server"
    
    if ($dataFiles.Count -eq 0) {
        Write-Log "No files to process"
        exit 0
    }
    
    # Process each file
    foreach ($dataFile in $dataFiles) {
        $localFilePath = [System.IO.Path]::Combine($filesDir, $dataFile)
        $archiveFilePath = [System.IO.Path]::Combine($archiveDir, $dataFile)
        
        # Check if file already processed
        $alreadyProcessed = Test-FileProcessed -FileName $dataFile -Connection $connection
        
        if ($alreadyProcessed) {
            Write-Log "File $dataFile already processed, skipping" -LogLevel "WARNING"
            continue
        }
        
        Write-Log "Processing file: $dataFile"
        
        # Download file from FTP
        Write-Log "Downloading file from FTP..."
        Get-FtpFile -RemoteFile $dataFile -LocalFile $localFilePath
        
        # Process the file
        Write-Log "Processing file contents..."
        $recordCount = Process-Zdmi027File -FilePath $localFilePath -FileName $dataFile -Connection $connection
        
        Write-Log "Successfully processed $recordCount records from file $dataFile"
        
        # Move local file to archive
        [System.IO.File]::Copy($localFilePath, $archiveFilePath, $true)
        [System.IO.File]::Delete($localFilePath)
        
        # Move file on FTP server to Complete folder
        try {
            Write-Log "Moving file on FTP server to Complete folder..."
            
            # Create FTP rename request (move to Complete folder)
            $sourcePath = "$ftpPath$dataFile"
            $destinationPath = "$ftpPath/Complete/$dataFile"
            
            $renameRequest = [System.Net.FtpWebRequest]::Create("ftp://$ftpServer$sourcePath")
            $renameRequest.Method = [System.Net.WebRequestMethods+Ftp]::Rename
            $renameRequest.RenameTo = "Complete/$dataFile"
            
            if ([string]::IsNullOrEmpty($ftpUser) -or [string]::IsNullOrEmpty($ftpPassword)) {
                $renameRequest.Credentials = New-Object System.Net.NetworkCredential("anonymous", "anonymous@domain.com")
            } else {
                $renameRequest.Credentials = New-Object System.Net.NetworkCredential($ftpUser, $ftpPassword)
            }
            
            $renameRequest.UsePassive = $true
            $renameRequest.KeepAlive = $false
            
            $response = $renameRequest.GetResponse()
            $response.Close()
            
            Write-Log "File successfully moved to Complete folder on FTP server"
        } catch {
            Write-Log ("WARNING: Could not move file on FTP server. Error: " + ${_}) -LogLevel "WARNING"
            Write-Log "Processing will continue as the file was successfully imported." -LogLevel "WARNING"
        }
        
        Write-Log "File processing and archiving completed"
    }
    
    Write-Log "ZDMI027 import process completed successfully"
}
catch {
    Write-Log ("ERROR: Process failed: " + ${_}) -LogLevel "ERROR"
    if ($_.ScriptStackTrace) {
        Write-Log ("Stack trace: " + $_.ScriptStackTrace) -LogLevel "ERROR"
    }
    
    # Exit with error
    exit 1
}
finally {
    # Clean up resources
    if ($null -ne $connection -and $connection.State -eq 'Open') {
        try { 
            $connection.Close() 
            Write-Log "Database connection closed"
        } catch {}
    }
}

Write-Log "Script execution complete"
