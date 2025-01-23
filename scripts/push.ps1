# Variables
$acrName = ""
$imageName = ""
$tag = ""
$dockerFilePath = "..\app"

Set-Location ..\app

az login

az acr login --name $acrName

# Build the Docker image
docker build -t "$acrName.azurecr.io/${imageName}:$tag" $dockerFilePath

# Push the Docker image to ACR
docker push "$acrName.azurecr.io/${imageName}:$tag"

Write-Host "Docker image '$acrName.azurecr.io/${imageName}:$tag' has been pushed to ACR."

Set-Location ..\scripts