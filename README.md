# SQL MCP Server

SQL MCP Server is a tool that enables large language models (LLMs) to interact with SQL Server databases through a standardized API. It provides a structured way for AI systems to query, analyze, and explore database structures using natural language.

## Features

- 🔒 **Secure Database Access**: Connect to Microsoft SQL Server databases with configurable security restrictions
- 🛠️ **Rich Tool Set**: Various database interaction tools for queries, schema exploration, and analysis 
- 📊 **Data Visualization Support**: Ability to export and analyze data in various formats
- 🧠 **AI-Ready Interface**: Implements the Model Context Protocol (MCP) for LLM integration
- ⚡ **Optimized Performance**: Connection pooling and efficiency optimizations for high throughput

## Installation

1. Clone this repository
2. Create a virtual environment: `python -m venv .venv`
3. Activate the virtual environment:
   - Windows: `.venv\Scripts\activate`
   - Linux/Mac: `source .venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Copy `.env.example` to `.env` and configure your database settings

## Configuration

Edit the `.env` file with your SQL Server connection details:

```
DB_SERVER=your_server_name
DB_NAME=your_database_name
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_ALLOWED_SCHEMAS=["dbo"]
```

## Usage

Start the server:

```
python sql_mcp_server.py
```

The server will be available at http://127.0.0.1:8000 by default. You can configure the host and port in the `.env` file.

## API Documentation

The SQL MCP Server implements tools for database interaction including:

- Basic schema exploration tools
- Advanced query building capabilities
- Data analysis and visualization support
- Security-focused access controls

## Security Considerations

- The server validates and sanitizes all SQL queries
- Read-only mode prevents data modification by default
- Schema restrictions limit access to specified database objects
- Connection pooling with timeout limits helps prevent resource exhaustion

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- The SQLAlchemy team for their excellent database toolkit
- The Model Context Protocol (MCP) specification
