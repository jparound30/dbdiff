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
A sample of `configuration.yaml`is as follows.

```yaml
db:
  type: postgresql (or mysql)
  host: localhost
  port: 5432
  user: username
  password: password
  name: sampledatabase
  schema: hoge.
```
### Run
1. Execute `dbdiff` on the command line.
```
dbdiff
```

2. Please operate accoding to the messages.

3. Output result to console, and generate Excel file(.xlsx) in the current directory.

## LIMITATIONS
- Tested only on macOS High Sierra / Go 1.11
