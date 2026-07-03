# Exporte architecture-hybride.svg en PNG et PDF (livrable Exercice 1, Étape 2).
# Le SVG reste la source versionnée ; PNG et PDF sont régénérables par ce script.
# Usage :  powershell -ExecutionPolicy Bypass -File docs\exercice1-modelisation\export-schema.ps1

$ErrorActionPreference = 'Stop'
$dir = $PSScriptRoot
$svg = Join-Path $dir 'architecture-hybride.svg'

# Navigateur Chromium (Chrome ou Edge) utilisé comme moteur de rendu SVG -> PNG/PDF
$candidats = @(
    'C:\Program Files\Google\Chrome\Application\chrome.exe',
    'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe',
    'C:\Program Files\Microsoft\Edge\Application\msedge.exe'
)
$navigateur = $candidats | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $navigateur) { throw "Aucun navigateur Chromium trouvé (Chrome ou Edge requis)." }

# Le SVG est inliné dans une page HTML pour figer la taille de page et supprimer les marges.
$tmp  = Join-Path $env:TEMP ("schema-export-" + [guid]::NewGuid().ToString('N') + '.html')
$html = "<meta charset='utf-8'><style>@page{size:1780px 1120px;margin:0}html,body{margin:0;padding:0;background:#fff}svg{display:block}</style>" +
        (Get-Content -Raw -Encoding UTF8 $svg)
Set-Content -Path $tmp -Value $html -Encoding UTF8
$url = 'file:///' + ($tmp -replace '\\', '/')

# PNG haute définition (facteur 2 => 3560 x 2240 px)
& $navigateur --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=2 `
    --window-size=1780,1120 --screenshot="$dir\architecture-hybride.png" $url | Out-Null

# PDF vectoriel
& $navigateur --headless=new --disable-gpu --no-pdf-header-footer `
    --print-to-pdf="$dir\architecture-hybride.pdf" $url | Out-Null

Remove-Item $tmp -Force
Get-ChildItem $dir -Filter 'architecture-hybride.*' | Select-Object Name, Length
