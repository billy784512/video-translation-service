## 1. Prerequistie

1. Azure Function (Container Apps environment)
2. Storage Account
3. Azure Event Hub
4. Azure Container registry (ACR)
5. Azure Speech Service
6. Azure Translator

## 2. Resource setup

### 2.1 Function app
Add env variable to your Azure Function, you can follow variables list in `.env.example`
```ps1
    EVENT_HUB_CONNECTION_STRING=  
    TRANSLATION_API_URL=    # Endpoint of Speech Service
    TRANSLATION_API_KEY=    # KEY of Speech Service

    TRANSLATOR_ENDPOINT=    # Endpoint of Translator
    TRANSLATOR_REGION=      # Region of Translator
    TRANSLATOR_KEY=         # Key of Translator
```

### 2.2 Event Hub
You need to prepare three event hub, their names should be:
1. translation
2. transoperationcheck
3. iteroperationcheck

## 3. How to start?

Make sure docker is available in your environment
```
    docker -v
```

Change to `\scripts`
``` ps1
    cd /scripts
```

Insert related information in `deploy.ps1` and `push.ps1`
(Below explain ACR related parameter)
``` ps1
    $acrName = ""       # The name of your Container registry
    $imageName = ""     # The name of image, you can name it whatever you like
    $tag = ""           # The tag of image  

    # Following info can be found in "Access keys" tab in Container registry
    $registryServerName = ""    # Login server
    $registryUserName = ""      # Username
    $registryPassword = ""      # password
```

Then, push image to ACR and deploy image to Azure Function through ACR 
``` ps1
    .\push.ps1
    .\deploy.ps1
```

After deploy process is done, you can call Azure Function by provided scrips
``` ps1
    .\req.ps1
``` 