$resourceGroup = ""
$functionAppName = ""
$acrName = ""
$imageName = ""
$tag = ""
$registryServerName = ""
$registryUserName = ""
$registryPassword = ""

az login

az functionapp config container set `
    --name $functionAppName `
    --resource-group $resourceGroup `
    --image "$acrName.azurecr.io/${imageName}:$tag" `
    --registry-server $registryServerName `
    --registry-username $registryUserName `
    --registry-password $registryPassword `
    --debug

Write-Host "Docker image '$acrName.azurecr.io/${imageName}:$tag' deployed to Function App '$functionAppName'."
