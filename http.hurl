# hurl --verbose --variable key=hello --variable value=world  http.hurl

#  Set
POST http://127.0.0.1:8080/api/entry/{{key}}
Content-Type: application/json
{
  "value": "{{value}}"
}
HTTP 200
[Asserts]
body contains "1"


# Get
GET http://127.0.0.1:8080/api/entry/{{key}}
Content-Type: application/json
HTTP 200

# Delete
DELETE http://127.0.0.1:8080/api/entry/{{key}}
HTTP 200
[Asserts]
body contains "1"

