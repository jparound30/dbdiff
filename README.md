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

```yaml
# PostgreSQL
db:
  type: postgresql
  host: localhost
  port: 5432
  user: username
  password: password
  name: sampledatabase
  schema: hoge.

# MySQL
db:
  type: mysql
  host: localhost
  port: 3306
  user: username
  password: password
  name: sampledatabase

# MS SQL Server
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
- Tested only on macOS High Sierra / Go 1.11
