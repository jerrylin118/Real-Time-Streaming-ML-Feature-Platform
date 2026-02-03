# Smoke test for Windows PowerShell
# Run after: docker compose up --build

$ErrorActionPreference = "Stop"

Write-Host "Waiting for inference to become healthy..."

$maxAttempts = 60
$attempt = 0
$inferenceReady = $false

while ($attempt -lt $maxAttempts) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            Write-Host "Inference is up"
            $inferenceReady = $true
            break
        }
    } catch {
        # Ignore
    }
    $attempt++
    Write-Host "Waiting for inference... ($attempt/$maxAttempts)"
    Start-Sleep -Seconds 2
}

if (-not $inferenceReady) {
    Write-Host "Inference failed to become healthy"
    exit 1
}

# Give producer/processor time to generate and process events
Write-Host "Waiting 15s for events to flow..."
Start-Sleep -Seconds 15

# Call inference
Write-Host "Calling inference endpoint..."
$body = '{"user_id": "u0", "item_id": "i0"}'
$response = Invoke-RestMethod -Uri "http://localhost:8000/predict" -Method POST -Body $body -ContentType "application/json"

Write-Host "Response: $($response | ConvertTo-Json)"

$prob = $response.probability
if ($null -eq $prob -or $prob -lt 0 -or $prob -gt 1) {
    Write-Host "FAIL: probability $prob is not between 0 and 1"
    exit 1
}

Write-Host "SUCCESS: probability=$prob is valid"
