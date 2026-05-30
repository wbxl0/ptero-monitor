FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/OWNER/ptero-monitor"
LABEL org.opencontainers.image.description="Pterodactyl Panel Server Monitor - Auto Keep-Alive"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Xray for vless/vmess/trojan/ss proxy support
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends wget unzip && \
    case ${TARGETARCH} in \
        amd64) XRAY_ARCH="64" ;; \
        arm64) XRAY_ARCH="arm64-v8a" ;; \
        *) echo "Unsupported arch: ${TARGETARCH}"; exit 1 ;; \
    esac && \
    wget -q "https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-${XRAY_ARCH}.zip" && \
    unzip -q "Xray-linux-${XRAY_ARCH}.zip" -d /tmp/xray && \
    mv /tmp/xray/xray /usr/local/bin/xray && \
    chmod +x /usr/local/bin/xray && \
    rm -rf /tmp/xray "Xray-linux-${XRAY_ARCH}.zip" && \
    apt-get remove -y wget unzip && apt-get autoremove -y && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy application
COPY app.py .
COPY static/ ./static/

# Create data directory
RUN mkdir -p /app/data

ENV PORT=8000
ENV DB_PATH=/app/data/monitor.db

EXPOSE 8000

VOLUME ["/app/data"]

CMD ["python", "app.py"]
