# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Prevent python from writing pyc files to disc
ENV PYTHONDONTWRITEBYTECODE 1
# Ensure python output is sent straight to the terminal without buffering
ENV PYTHONUNBUFFERED 1

# Set the Playwright browser path environment variable.
# This ensures that browsers are installed in a shared location (/ms-playwright)
# that is consistent with the runtime environment variable set in render.yaml.
ENV PLAYWRIGHT_BROWSERS_PATH=0

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Essential packages
    ca-certificates \
    wget \
    curl \
    gnupg \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers with system dependencies
RUN python -m playwright install --with-deps chromium

# Copy the rest of the application code into the container
COPY . .

# Command to run the application using a threaded worker for background tasks
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:10000", "--workers", "1", "--threads", "4", "--timeout", "300", "--worker-class", "gthread"]