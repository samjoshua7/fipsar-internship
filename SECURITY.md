# Security Best Practices

This document outlines security best practices for working with this repository. Please follow these guidelines to protect sensitive information.

## 🔐 Sensitive Information Management

### Environment Variables

**NEVER commit the following to version control:**

- `.env` files with actual credentials
- Database passwords and connection strings
- API keys and tokens
- Encryption keys (Fernet keys, JWT secrets)
- Private certificates or SSH keys
- AWS, Azure, GCP, or other cloud credentials
- Authentication credentials

### Using Environment Variables

All sensitive information is managed through environment files. Each module has a `.env.example` file showing the required variables:

- `api-data-ingestion/.env.example` - Application configuration
- `api-data-ingestion/airflow/.env.example` - Airflow configuration

**Setup Instructions:**

1. Copy `.env.example` to `.env` in the same directory
2. Update placeholder values with your actual credentials
3. Never commit the `.env` file (it's in `.gitignore`)

```bash
# Example for api-data-ingestion/
cp api-data-ingestion/.env.example api-data-ingestion/.env
# Edit api-data-ingestion/.env with your credentials
```

## Airflow Secrets Management

### Setting Up Airflow

1. **Generate a secure Fernet key:**

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

2. **Create `api-data-ingestion/airflow/.env` with:**

```env
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=your_strong_password_here
FERNET_KEY=your_generated_fernet_key_here
AIRFLOW__API_AUTH__JWT_SECRET=your_jwt_secret_here
AIRFLOW__API_AUTH__JWT_ISSUER=airflow
```

3. **Create `api-data-ingestion/.env` with:**

```env
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=api_ingestion
DB_USER=postgres
DB_PASSWORD=your_database_password_here
API_URL=https://jsonplaceholder.typicode.com/posts
```

### Database Configuration

- Default credentials (`postgres`/`airflow`) are **development only**
- For local development with Docker, credentials in `.env` override defaults
- For production, set environment variables in your deployment system:
  - Never use hard-coded credentials
  - Use secrets management services (AWS Secrets Manager, Azure Key Vault, etc.)

## 🚨 Pre-Commit Checklist

Before pushing to GitHub, ensure:

- [ ] No `.env` files are staged (should be in `.gitignore`)
- [ ] No `.pem`, `.key`, `.pfx`, or `.p12` files are staged
- [ ] No hard-coded passwords or API keys in code
- [ ] No database credentials in application files
- [ ] `.gitignore` is comprehensive and up-to-date

**Check staged files:**

```bash
git diff --cached --name-only
```

## Credential Rotation

If any credentials are accidentally exposed:

1. **Immediately rotate credentials:**
   - Database passwords
   - API keys and tokens
   - Encryption keys
   - Admin account passwords

2. **Review git history:**

```bash
# Search for exposed credentials in history
git log -p -S "password" -- "*.py"
git log -p -S "secret" -- "*.env"
```

3. **If credentials were committed:**

   - Consider the repository compromised
   - Use BFG Repo-Cleaner or git-filter-branch to remove from history
   - Force push (if appropriate for your workflow)
   - Rotate all exposed credentials
   - Re-key encryption systems

## Secret Detection Tools

Consider using these tools to prevent accidental commits:

- **git-secrets** - Prevents you from committing secrets
- **pre-commit** - Framework for managing git hooks
- **TruffleHog** - Scans repositories for credentials

### Setup git-secrets

```bash
# Install
brew install git-secrets

# Configure repo
cd fipsar-internship
git secrets --install
git secrets --register-aws

# Test
git secrets --scan
```

## 📋 Files Safe to Commit

These files contain no sensitive information:

- `LICENSE`
- `README.md`
- `SECURITY.md`
- `.gitignore`
- Python source files (if no hardcoded secrets)
- SQL query files
- YAML configuration templates (without credentials)
- `*.example` files

## ⛔ Files That Must NOT Be Committed

- `.env` and `.env.*` files (except `.env.example`)
- `airflow.db` and other database files
- `airflow/simple_auth_manager_passwords.json*`
- `airflow/webserver_config.py` (if contains secrets)
- `*.key`, `*.pem`, `*.p12`, `*.pfx` files
- SSH keys or private certificates
- Logs with sensitive information

## Configuration Security

### Airflow Configuration

- `load_examples = False` for production deployments
- Use environment variables for sensitive settings
- Never hard-code Fernet keys or authentication secrets
- Enable secret masking in logs (already configured)

### Docker Configuration

- Never pass secrets as plain text in `docker-compose.yaml`
- Use environment variables and `.env` files
- Use Docker Secrets for production orchestration
- Consider secrets management solutions for multi-host deployments

## Database Security

- Change default PostgreSQL credentials (user: `airflow`, password: `airflow`)
- Use strong passwords (minimum 16 characters, mixed case, numbers, symbols)
- Restrict database access to only required services
- Regularly backup and secure backups
- Use encryption for data at rest and in transit

## Additional Resources

- [OWASP: Secrets Management](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Airflow Security](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/)
- [Docker Security Best Practices](https://docs.docker.com/develop/security-best-practices/)
- [PostgreSQL Security](https://www.postgresql.org/docs/current/sql-security.html)

## Questions or Issues?

If you suspect a security issue or have questions about these practices, please:

1. Do not publicly disclose security vulnerabilities
2. Contact the repository maintainer privately
3. Provide detailed information about the issue
4. Allow time for a fix before public disclosure

---

**Last Updated:** 2026-06-20  
**Repository:** fipsar-internship
