# Environment Variables Setup

## Quick Start

1. **Copy the example file**:

   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your credentials**:

   ```bash
   # On Windows
   notepad .env
   
   # On Linux/Mac
   nano .env
   ```

3. **Update the password**:

   ```env
   POSTGRES_PASSWORD=your_secure_password_here
   ```

## Security Notes

- **`.env`** is in `.gitignore` and will NOT be committed to Git
- **`.env.example`** is a template and SHOULD be committed
- Never commit real credentials to version control
- For production, use secrets management (e.g., AWS Secrets Manager, HashiCorp Vault)

## How It Works

Docker Compose automatically reads the `.env` file and substitutes variables:

```yaml
environment:
  - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}  # Reads from .env
```

## Accessing Environment Variables in Python

```python
import os
from dotenv import load_dotenv

# Load .env file (for local development outside Docker)
load_dotenv()

# Access variables
db_password = os.getenv("POSTGRES_PASSWORD")
db_host = os.getenv("POSTGRES_HOST", "localhost")  # with default value
```

## Troubleshooting

### Variables not loading?

```bash
# Check if .env exists
ls -la .env

# Restart containers to pick up changes
docker-compose down
docker-compose up -d
```

### Still using old values?

```bash
# Force rebuild
docker-compose down
docker-compose up -d --build --force-recreate
```
