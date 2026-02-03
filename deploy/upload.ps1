param(
  [Parameter(Mandatory=$true)][string]$Server,
  [Parameter(Mandatory=$true)][string]$User,
  [string]$RemoteDir = "/home/$User/schedule-bot"
)

$files = @(
  "main.py",
  "requirements.txt",
  "README.md",
  ".env.example"
)

Write-Host "Uploading to $User@$Server:$RemoteDir"
ssh "$User@$Server" "mkdir -p '$RemoteDir/deploy'"
scp $files "$User@$Server`:$RemoteDir/"
scp "deploy/schedule-bot.service" "$User@$Server`:$RemoteDir/deploy/"
