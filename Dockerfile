FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Make entrypoint executable
RUN chmod +x start.sh

# Expose dashboard port (Koyeb reads PORT env var)
EXPOSE 8000

# Run scanner + dashboard together
CMD ["/bin/bash", "start.sh"]
