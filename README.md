# Infra DB to Arango DB Syncer

## Summary
This project aims to sync the data from the Infra Db database to an Arango DB database.
The data is stored in the Infra Db database in a specific format. The data is retrieved through the API and stored in the Arango DB database.

## Installation

### 1. Add the ArangoDB repository key
```
curl -OL https://download.arangodb.com/9c169fe900ff79790395784287bfa82f0dc0059375a34a2881b9b745c8efd42e/arangodb312/DEBIAN/Release.key
sudo apt-key add - < Release.key
```
### 2. Add the repository and install ArangoDB
```
echo 'deb https://download.arangodb.com/9c169fe900ff79790395784287bfa82f0dc0059375a34a2881b9b745c8efd42e/arangodb312/DEBIAN/ /' | sudo tee [/etc/apt/sources.list.d/arangodb.list](VALID_FILE)
sudo apt-get install apt-transport-https
sudo apt-get update
sudo apt-get install arangodb3e=3.12.5.2-1
```
To install the debug symbols package (optional):
```
sudo apt-get install arangodb3e-dbg=3.12.5.2-1
```
### 3. Start and enable the database service
```
sudo systemctl start arangodb3
sudo systemctl enable arangodb3
```
### 4. Access the Web UI
Open your browser and go to [http://localhost:8529](http://localhost:8529).
Log in with root and the password you set during installation.

### 5. Create a database
```
curl -u root:yourpassword -X POST http://localhost:8529/_api/database -d '{"name": "infra_db"}'
```
### 6. Create a user
```
curl -u root:yourpassword \
  -X POST http://localhost:8529/_api/user \
  -H "Content-Type: application/json" \
  -d '{
    "user": "sync_user",
    "passwd": "sync_passwrd",
    "active": true
  }'
```
### 7. Grant access to the user
```
curl -u root:yourpassword \
  -X PUT http://localhost:8529/_api/user/sync_user/database/infra_db \
  -H "Content-Type: application/json" \
  -d '{
    "grant": "rw"
  }'
```

## Datamodel
