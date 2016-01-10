# docker-compose usage

## docker-compose.yml

```yml
elasticsearch-log-export
  build: https://github.com/gallna/elasticsearch-log-export.git
  ports:
    - "8089:8089"
  environment:
    AWS_ACCESS_KEY_ID: "aws_access_key_id"
    AWS_SECRET_ACCESS_KEY: "aws_secret_access_key"
```


$ docker-compose up
http://localhost:8089/export/BUCKET/PREFIX?endpoint=elasticsearch&indexName=index-name&typeName=type-name
