# Exporte evaluation-compatibilite.md en PDF A4 (livrable Exercice 1, Étape 3).
# Le Markdown reste la source versionnée ; le PDF est régénérable par ce script.
# Prérequis : Python 3 + mistune (pip install mistune), Chrome ou Edge.
# Usage :  powershell -ExecutionPolicy Bypass -File docs\exercice1-modelisation\export-evaluation.ps1

$ErrorActionPreference = 'Stop'
$dir = $PSScriptRoot
$md  = Join-Path $dir 'evaluation-compatibilite.md'

$candidats = @(
    'C:\Program Files\Google\Chrome\Application\chrome.exe',
    'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe',
    'C:\Program Files\Microsoft\Edge\Application\msedge.exe'
)
$navigateur = $candidats | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $navigateur) { throw "Aucun navigateur Chromium trouvé (Chrome ou Edge requis)." }

$tmp = Join-Path $env:TEMP ("evaluation-" + [guid]::NewGuid().ToString('N') + '.html')
python (Join-Path $dir 'md2html.py') $md $tmp `
    "Évaluation de compatibilité de l'infrastructure hybride - InduTechData"
if ($LASTEXITCODE -ne 0) { throw "Échec de la conversion Markdown -> HTML." }

$url = 'file:///' + ($tmp -replace '\\', '/')
& $navigateur --headless=new --disable-gpu --no-pdf-header-footer `
    --print-to-pdf="$dir\evaluation-compatibilite.pdf" $url | Out-Null

Remove-Item $tmp -Force
Get-ChildItem "$dir\evaluation-compatibilite.pdf" | Select-Object Name, Length
