# [dbdiff] Tool to detect data difference in the database

## Configuration
1. Put `configuration.yaml` file in the current directory.  
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
2. Execute `dbdiff` on the command line.
```
dbdiff
```

3. Please operate accoding to the messages.

4. Output result to console, and generate Excel file(.xlsx) in the current directory.

