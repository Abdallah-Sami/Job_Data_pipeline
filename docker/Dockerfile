FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

RUN apt-get update \
 && apt-get install -y --no-install-recommends chromium chromium-driver \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tanqeeb_scraper_docker.py .

# Run as non-root
RUN useradd -m scraper
USER scraper

CMD ["python", "tanqeeb_scraper_docker.py"]
