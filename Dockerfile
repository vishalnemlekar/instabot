# Playwright’s official Python image (includes Chromium/Firefox/WebKit + OS deps)
FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

WORKDIR /app

# Copy and install Python deps (PTB 21, supabase, dotenv, optional supervisor)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your project files
COPY . .

# If you want to run BOTH scraper and bot under supervisor (recommended):
# supervisor is already in requirements.txt; otherwise uncomment:
# RUN pip install --no-cache-dir supervisor==4.2.5

# Default: use supervisord to run both processes
CMD ["supervisord", "-c", "/app/supervisord.conf"]
