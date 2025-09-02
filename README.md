# Infra DB to Arango DB Syncer

## Summary
This project aims to sync the data from the Infra Db database to an Arango DB database.
The data is stored in the Infra Db database in a specific format. The data is retrieved through the API and stored in the Arango DB database.

## Installation

### 1. Add the ArangoDB repository key
```
# Create keyrings dir if it doesn't exist
sudo mkdir -p /etc/apt/keyrings

# Download and store the GPG key securely
curl -fsSL https://download.arangodb.com/9c169fe900ff79790395784287bfa82f0dc0059375a34a2881b9b745c8efd42e/arangodb312/DEBIAN/Release.key \
  | gpg --dearmor \
  | sudo tee /etc/apt/keyrings/arangodb.gpg > /dev/null
```
### 2. Add the repository and install ArangoDB
```
echo "deb [signed-by=/etc/apt/keyrings/arangodb.gpg] https://download.arangodb.com/arangodb312/DEBIAN/ /" \
  | sudo tee /etc/apt/sources.list.d/arangodb.list
sudo apt-get install apt-transport-https
sudo apt-get update
sudo apt-get install arangodb3
```
To install the debug symbols package (optional):
```
sudo apt-get install arangodb3-dbg
```
### 3. Start and enable the database service
```
sudo systemctl start arangodb3
sudo systemctl enable arangodb3
```
### 4. Access the Web UI
Open your browser and go to [http://localhost:8529](http://localhost:8529) for the web interface.
Log in with root and the password you set during installation.

### 5. Create a database
_(change the database name accordingly)_
```
curl -u root:yourpassword -X POST http://localhost:8529/_api/database -d '{"name": "infra_db"}'
```
### 6. Create a user
_(change the username and password accordingly)_
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
_(change the username, password and database name accordingly)_
```
curl -u root:yourpassword \
  -X PUT http://localhost:8529/_api/user/sync_user/database/infra_db \
  -H "Content-Type: application/json" \
  -d '{
    "grant": "rw"
  }'
```

## Datamodel
