# Use an official Python image as the base
FROM python:3.9

# Set the working directory
WORKDIR /app

# Install required dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    mariadb-client \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages for database interaction
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the scripts and dataset
COPY . /app

# Set the container to keep running
CMD ["bash"]
