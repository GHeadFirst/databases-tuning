#!/bin/bash

# Wait for PostgreSQL and MariaDB to be ready
echo "Waiting for PostgreSQL and MariaDB to be ready..."

# Loop until PostgreSQL responds
while ! nc -z postgres_db 5432; do
  sleep 1
done
echo "PostgreSQL is ready!"

# Loop until MariaDB responds
while ! nc -z mariadb_db 3306; do
  sleep 1
done
echo "MariaDB is ready!"

# Run the main application script
echo "Starting the application..."
exec "$@"
