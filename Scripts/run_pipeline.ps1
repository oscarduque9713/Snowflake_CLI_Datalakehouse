
# Local paths based on current Windows user
$UserProfile = $env:USERPROFILE

# Folders
$ProjectPath = Join-Path $UserProfile "Documents\Project_snowflake_cli"
$PythonPath  = Join-Path $ProjectPath "Python_Script"
$BasePath     = Join-Path $ProjectPath "SQL"
$LocalDataPath = Join-Path $ProjectPath "data_sample"

# Normalize path for Snowflake PUT
$LocalDataPathForSnowflake = $LocalDataPath.Replace('\', '/').TrimEnd('/')

$PythonScript = Join-Path $PythonPath "mover_files.py"

# Snowflake parameters and connection
$Connection = "developer"
$DbName = "PROJECT_SEMESTRUCTURED"

Write-Host "Project path: $ProjectPath"
Write-Host "Python path: $PythonPath"
Write-Host "SQL path: $BasePath"
Write-Host "Python script: $PythonScript"
Write-Host "Local data path Snowflake: $LocalDataPathForSnowflake"


# Stop PowerShell when an unexpected error occurs
$ErrorActionPreference = "Stop"

# ============================================================
# Function: Execute Python script
# ============================================================

function Run-PythonScript {
    param (
        [string]$StepName,
        [string]$ScriptPath
    )

    Write-Host ""
    Write-Host "============================================================"
    Write-Host $StepName
    Write-Host "Python script: $ScriptPath"
    Write-Host "============================================================"

    if (-not (Test-Path $ScriptPath)) {
        Write-Host "ERROR: No existe el script Python: $ScriptPath"
        exit 1
    }

    python "$ScriptPath"

    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Fallo el script Python: $ScriptPath"
        exit 1
    }

    Write-Host "Python finalizo correctamente."
}

# ============================================================
# Function: Execute SQL script with Snowflake CLI
# ============================================================

function Run-SnowflakeCliScript {
    param (
        [string]$StepName,
        [string]$ScriptName,
        [string]$BatchId = ""
    )

    $FilePath = Join-Path -Path $BasePath -ChildPath $ScriptName

    Write-Host ""
    Write-Host "============================================================"
    Write-Host $StepName
    Write-Host "Archivo SQL: $FilePath"
    Write-Host "Database: $DbName"
    Write-Host "Local data path: $LocalDataPathForSnowflake"
    Write-Host "============================================================"

    if (-not (Test-Path $FilePath)) {
        Write-Host "ERROR: No existe el archivo: $FilePath"
        exit 1
    }

    if ((Get-Item $FilePath).PSIsContainer) {
        Write-Host "ERROR: La ruta apunta a una carpeta, no a un archivo: $FilePath"
        exit 1
    }

    if ($BatchId -ne "") {
        Write-Host "Usando Batch ID: $BatchId"

        snow sql `
            -f "$FilePath" `
            --connection $Connection `
            -D "DB_NAME=$DbName" `
            -D "LOCAL_DATA_PATH=$LocalDataPathForSnowflake" `
            -D "BATCH_ID=$BatchId"
    }
    else {
        snow sql `
            -f "$FilePath" `
            --connection $Connection `
            -D "DB_NAME=$DbName" `
            -D "LOCAL_DATA_PATH=$LocalDataPathForSnowflake"
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Fallo el script $FilePath"
        exit 1
    }

    Write-Host "Script SQL ejecutado correctamente: $ScriptName"
}

# ============================================================
# Function: Get current running Batch ID
# ============================================================

function Get-RunningBatchId {
    $BatchQuery = "SELECT BATCH_ID FROM $DbName.BRONZE.PIPELINE_BATCH_CONTROL WHERE STATUS = 'RUNNING' ORDER BY START_TS DESC LIMIT 1;"

    Write-Host ""
    Write-Host "Obteniendo Batch ID desde Snowflake..."

    $RawOutput = snow sql `
        -q "$BatchQuery" `
        --connection $Connection `
        --format json

    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: No se pudo consultar el Batch ID."
        exit 1
    }

    try {
        $JsonOutput = $RawOutput | ConvertFrom-Json

        # Snowflake CLI JSON output can vary by version.
        # This tries to extract the first BATCH_ID value from the response.
        $BatchIdValue = $null

        if ($JsonOutput.data) {
            $BatchIdValue = $JsonOutput.data[0].BATCH_ID
        }
        elseif ($JsonOutput[0].data) {
            $BatchIdValue = $JsonOutput[0].data[0].BATCH_ID
        }
        elseif ($JsonOutput[0].BATCH_ID) {
            $BatchIdValue = $JsonOutput[0].BATCH_ID
        }

        if ([string]::IsNullOrWhiteSpace($BatchIdValue)) {
            Write-Host "ERROR: No se encontro un Batch ID con estado RUNNING."
            Write-Host "Output recibido:"
            Write-Host $RawOutput
            exit 1
        }

        return $BatchIdValue.Trim()
    }
    catch {
        Write-Host "ERROR: No se pudo interpretar la salida JSON del comando snow sql."
        Write-Host "Output recibido:"
        Write-Host $RawOutput
        exit 1
    }
}

# ============================================================
# Pipeline execution
# ============================================================

Write-Host ""
Write-Host "============================================================"
Write-Host "Starting pipeline with Snowflake CLI"
Write-Host "Connection: $Connection"
Write-Host "Project path: $ProjectPath"
Write-Host "Python path: $PythonPath"
Write-Host "SQL path: $BasePath"
Write-Host "Local data path: $LocalDataPath"
Write-Host "============================================================"

# 0. Execute local Python first
Run-PythonScript `
    -StepName "0. Ejecutando Python para preparar/mover archivos..." `
    -ScriptPath $PythonScript

# 1. Prepare environment
Run-SnowflakeCliScript `
    -StepName "1. Ejecutando preparacion de ambiente..." `
    -ScriptName "0.Prep_Env.sql"

# 2. Creacion Stored Procedures
Run-SnowflakeCliScript `
    -StepName "2. Ejecutando Creacion Stored Procedures..." `
    -ScriptName "1.Create_Procedures.sql"

# 3 Start batch
Run-SnowflakeCliScript `
    -StepName "3 Creando batch en Snowflake..." `
    -ScriptName "2.Start_Batch.sql"

# Get current running batch id
$BatchId = Get-RunningBatchId

Write-Host ""
Write-Host "Batch ID desde Snowflake: $BatchId"

# 4. Load files to stage
Run-SnowflakeCliScript `
    -StepName "4. Subiendo archivos al stage..." `
    -ScriptName "3.Load_files_stage.sql" `
    -BatchId $BatchId

# 5. Ingest
Run-SnowflakeCliScript `
    -StepName "5. Ejecutando ingesta..." `
    -ScriptName "4.Ingest.sql" `
    -BatchId $BatchId

# 6 Validate batch load
Run-SnowflakeCliScript `
    -StepName "6 Validando registros cargados..." `
    -ScriptName "5.Validate_Ingest.sql" `
    -BatchId $BatchId

# 7 Update counts
Run-SnowflakeCliScript `
    -StepName "7 Actualizando conteos batch..." `
    -ScriptName "6.Validate_Batch_Load.sql" `
    -BatchId $BatchId

# 8. Transform
Run-SnowflakeCliScript `
    -StepName "8. Ejecutando transformacion..." `
    -ScriptName "7.Transform_Silver.sql" `
    -BatchId $BatchId


# 9. Business rules
Run-SnowflakeCliScript `
    -StepName "9. Ejecutando business rules..." `
    -ScriptName "8.Business_rules_enrich_information.sql" `
    -BatchId $BatchId

# 10. Gold
Run-SnowflakeCliScript `
    -StepName "10. Ejecutando Gold..." `
    -ScriptName "9.Load_Gold.sql" `
    -BatchId $BatchId


# 11. End batch success
Run-SnowflakeCliScript `
    -StepName "11. Ejecutando validacion successful batch control..." `
    -ScriptName "10.End_Batch_Success.sql" `
    -BatchId $BatchId

Write-Host ""
Write-Host "============================================================"
Write-Host "Pipeline ejecutado correctamente."
Write-Host "Batch ID: $BatchId"
Write-Host "============================================================"