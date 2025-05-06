# SQL Connection Help

This document provides guidance for configuring and troubleshooting your connection to SQL Server.

## Configuration Requirements

To use the SQL MCP Server, you need to configure the following:

1. **SQL Server Instance**
   - Connection string to your SQL Server instance
   - Proper network access to the server

2. **Authentication**
   - Username and password with appropriate permissions
   - Currently only SQL Authentication is supported

3. **Database Access**
   - Access to the specific database you want to query
   - Permission to access the schemas you need

## Configuration Steps

1. Create a `.env` file in the root directory of the project
2. Add the following configuration (using your actual values):
   ```
   DB_SERVER=your_server_name
   DB_NAME=your_database_name
   DB_USERNAME=your_username
   DB_PASSWORD=your_password
   DB_ALLOWED_SCHEMAS=["dbo"]
   ```

## Troubleshooting Connection Issues

### Common Issues

#### "Login failed for user"
- Verify the username and password are correct
- Ensure the SQL user has permission to access the database
- Check if the SQL Server is configured to allow SQL authentication

#### "Cannot connect to server"
- Verify the server name is correct
- Check network connectivity to the server
- Ensure firewall rules allow the connection
- Verify the SQL Server service is running

#### "Database not accessible"
- Verify the database name is correct
- Ensure the user has permission to access the specified database

#### "Schema access denied"
- Verify the schema name is correct
- Ensure the user has permission to access the specified schema
- Check that the schema is included in the `DB_ALLOWED_SCHEMAS` setting

## Required SQL Server Permissions

The SQL user needs the following minimum permissions:

- SELECT permission on all tables and views you want to access
- EXECUTE permission on any stored procedures you want to call
- VIEW DEFINITION permission to see table schemas

## Testing Your Connection

You can verify your SQL Server connection by running:

```
python main.py
```

If the server starts successfully, your connection is configured correctly.

## Getting Help

If you continue to experience connection issues, check your SQL Server configuration and consult with your database administrator.
