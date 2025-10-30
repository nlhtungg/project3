# Superset configuration file

# Flask App Builder configuration
# Your App secret key will be used for securely signing the session cookie
SECRET_KEY = 'supersetsecretkey'

# The SQLAlchemy connection string to your database backend
# This connection defines the path to the database that stores your
# superset metadata (slices, connections, tables, dashboards, ...).
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset@postgres:5432/superset'

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = ''

# Trino connection settings
TRINO_HOST = 'trino-coordinator'
TRINO_PORT = 8080
TRINO_CATALOG = 'delta'  # or 'hive' depending on your use case
TRINO_SCHEMA = 'default'

# Enable feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
}

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# Additional Trino configurations
ALLOWED_EXTRA_CONNECTIONS = {
    'trino': {
        'allow': True
    }
}
