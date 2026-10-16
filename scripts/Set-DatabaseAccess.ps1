param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Roles', 'Grants')]
    [string]$Phase,
    [ValidatePattern('^[a-zA-Z0-9_]*$')]
    [string]$Database = ''
)
$ErrorActionPreference = 'Stop'
# Pass environment variable names only; never place passwords in command-line arguments.
$required = @('WALLET_MIGRATOR_USER', 'WALLET_WORKER_USER')
if ($Phase -eq 'Roles') { $required += @('WALLET_MIGRATOR_PASSWORD', 'WALLET_WORKER_PASSWORD') }
foreach ($name in $required) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))) { throw "Missing $name" }
}
$projectRoot = Split-Path $PSScriptRoot -Parent
$sqlPath = Join-Path $projectRoot ('deploy/postgres/' + $Phase.ToLowerInvariant() + '.sql')
$dockerArgs = @('compose', 'exec', '-T')
foreach ($name in $required) { $dockerArgs += @('-e', $name) }
$dockerArgs += @('postgres', 'sh', '-c', 'exec psql -X -U "$POSTGRES_USER" -d "${1:-$POSTGRES_DB}" -v ON_ERROR_STOP=1 -f -', 'sh')
if ($Database) { $dockerArgs += $Database }
Push-Location $projectRoot
try {
    Get-Content -LiteralPath $sqlPath -Raw -Encoding UTF8 | & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Database access provisioning failed with exit code $LASTEXITCODE" }
} finally { Pop-Location }
