## Kola

It's a super simple, not really useful db, goal is to play around and try implementing some working db e2e.

### API
```
GET /{key},
    result: 200 with value in body, 404 if not found, 5XX in case of other issues
PUT /{key},
    body: value
    result: 200 if OK, 4XX/5XX depending on the kind of issue
```