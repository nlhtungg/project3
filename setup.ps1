function Download-WithProgress {
    param (
        [string]$Url,
        [string]$OutputPath
    )

    $request = [System.Net.HttpWebRequest]::Create($Url)
    $response = $request.GetResponse()
    $totalBytes = $response.ContentLength
    $stream = $response.GetResponseStream()
    $fileStream = New-Object System.IO.FileStream($OutputPath, [System.IO.FileMode]::Create)

    $buffer = New-Object byte[] 8192
    $bytesRead = 0
    $totalRead = 0

    while (($bytesRead = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
        $fileStream.Write($buffer, 0, $bytesRead)
        $totalRead += $bytesRead
        $percent = [math]::Round(($totalRead / $totalBytes) * 100, 2)
        Write-Progress -Activity "Downloading $OutputPath" -Status "$percent% Complete" -PercentComplete $percent
    }

    $fileStream.Close()
    $stream.Close()
    $response.Close()
    Write-Host "Download complete: $OutputPath"
}

# Create the jars directory if it doesn't exist
$folder = "jars"
if (-not (Test-Path $folder)) {
    New-Item -ItemType Directory -Path $folder
}

# Define URLs and filenames
$files = @{
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" = "hadoop-aws-3.3.4.jar"
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" = "aws-java-sdk-bundle-1.12.262.jar"
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar" = "postgresql-42.7.7.jar"
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" = "delta-spark_2.12-3.2.0.jar"
    "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" = "delta-storage-3.2.0.jar"
}

# Download only missing files with progress
foreach ($url in $files.Keys) {
    $output = Join-Path $folder $files[$url]
    if (-not (Test-Path $output)) {
        Write-Host "Downloading $($files[$url])..."
        Download-WithProgress -Url $url -OutputPath $output
    } else {
        Write-Host "$($files[$url]) already exists. Skipping download."
    }
}
