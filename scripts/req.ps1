
$url = "" # call the url of "video-translation" function
$method = "POST"
$headers = @{
    "Content-Type" = "application/json"
}

$body = @{
    "blob" = @{
        "conn_str" = ""
        "source_container_name" = ""
        "target_container_name" = ""
        "blob_name" = "" 
    }
    "lang" = @{
        "source" = "en-US"
        "target" = "zh-TW"
    }
    "chunk_size" = 100
    "pass_to_eventhub" = "true"
    "mode" = "native"
    "with_subtitle" = "true"
    # "category_id" = "" # optional
} | ConvertTo-Json -Depth 10

try {
    $response = Invoke-WebRequest -Uri $url -Method $method -Headers $headers -Body $body -TimeoutSec 3600
    $response
} catch {
    Write-Host "HTTP Error Details:" -ForegroundColor Red
    Write-Host "Status Code: $($_.Exception.Response.StatusCode)" -ForegroundColor Yellow
    Write-Host "Status Description: $($_.Exception.Response.StatusDescription)" -ForegroundColor Yellow

    $errorResponse = $_.Exception.Response.GetResponseStream()
    if ($errorResponse) {
        $reader = New-Object System.IO.StreamReader($errorResponse)
        $errorContent = $reader.ReadToEnd()
        Write-Host "Error Content: $errorContent" -ForegroundColor Yellow
    }
}
