go build --buildmode=plugin -o srtr_service.so srtr_service.go
go build --buildmode=plugin -o srtm_service.so srtm_service.go

go build --buildmode=plugin -o wcm_service.so wcm_service.go
go build --buildmode=plugin -o wcr_service.so wcr_service.go

go build --buildmode=plugin -o grepr_service.so grepr_service.go
go build --buildmode=plugin -o grepm_service.so grepm_service.go