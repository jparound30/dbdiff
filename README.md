# [dbdiff] Tool to detect data difference in the database

## How to setup
### Install
```
go get github.com/jparound30/dbdiff/cmd/dbdiff
```
### or Update
```
go get -u github.com/jparound30/dbdiff/cmd/dbdiff
```

### Configuration
Put `configuration.yaml` file in the current directory.  
Or specify a configuration file path by `-conf` option. 

A sample of `configuration.yaml`is as follows.

#### PostgreSQL
```yaml
db:
  type: postgresql
  host: localhost
  port: 5432
  user: username
  password: password
  name: sampledatabase
  schema: hoge.
```
#### MySQL
```yaml
db:
  type: mysql
  host: localhost
  port: 3306
  user: username
  password: password
  name: sampledatabase
```
#### MS SQL Server
```yaml
db:
  type: mssql
  host: localhost
  port: 1433
  user: username
  password: password
  name: sampledatabase
```
### Run
1. Execute `dbdiff` on the command line.
```
dbdiff
```
Usage:
```
  -conf string
        Specify path of configuration file. (default "configuration.yaml")
  -o string
        Filename of result file(.xlsx). (default "dbdiff_yyyymmdd_hhmmss.xlsx")
```
2. Please operate accoding to the messages.

3. Output result to console, and generate Excel file(.xlsx) in the current directory.

## LIMITATIONS
- Tested on macOS Catalina / Go 1.13
- Tested on Windows 10 Ver.1909 / Go 1.13
- Tested on Ubuntu 19.10  / Go 1.13
