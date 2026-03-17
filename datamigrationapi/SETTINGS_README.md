# Settings

## Production
`settings.py` is the main settings file used in production as-is.
`DJANGO_SETTINGS_MODULE=datamigrationapi.settings`

## Local Development
Create `datamigrationapi/local_settings.py` (gitignored) to override
any setting for local development. It is auto-loaded at the bottom
of `settings.py` if present.

## Environment Variables
Sensitive values (SECRET_KEY, DB passwords) should be in `.env`
loaded via python-dotenv. Never hardcode them in `settings.py`.
