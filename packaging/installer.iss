#define AppVersion "1.0.0"
[Setup]
AppId={{2F68B378-F264-4BB6-9E71-988FB1B9E9DC}
AppName=WAN Batch Studio
AppVersion={#AppVersion}
AppPublisher=mysticalg
AppPublisherURL=https://github.com/mysticalg/WAN-Batch-Studio
DefaultDirName={localappdata}\Programs\WAN Batch Studio
DefaultGroupName=WAN Batch Studio
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
OutputDir=..\release
OutputBaseFilename=WAN-Batch-Studio-Setup-{#AppVersion}-x64
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
LicenseFile=..\LICENSE
UninstallDisplayIcon={app}\WANBatchStudio.exe
CloseApplications=yes
[Tasks]
Name: desktopicon; Description: "Create a desktop shortcut"; Flags: unchecked
[Files]
Source: "..\dist\WANBatchStudio\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
[Icons]
Name: "{group}\WAN Batch Studio"; Filename: "{app}\WANBatchStudio.exe"
Name: "{autodesktop}\WAN Batch Studio"; Filename: "{app}\WANBatchStudio.exe"; Tasks: desktopicon
[Run]
Filename: "{app}\WANBatchStudio.exe"; Description: "Open WAN Batch Studio"; Flags: nowait postinstall skipifsilent
