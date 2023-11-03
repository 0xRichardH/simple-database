# hurl --verbose --variable key=hello --variable value=world  http.hurl

#  Set
POST http://127.0.0.1:8080/api/set/{{key}}
Content-Type: application/json
{
  "value": "{{value}}"
}
HTTP 200
[Asserts]
body contains "1"


# Get
GET http://127.0.0.1:8080/api/get/{{key}}
Content-Type: application/json
HTTP 200
