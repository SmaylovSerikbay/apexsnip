# Сборка и запуск всего пакета (main.go + pump_live.go + ...)
Set-Location $PSScriptRoot
go run -mod=vendor .
