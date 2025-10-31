#!/bin/bash

# Script to generate mTLS certificates for development/testing
# Creates CA, server, and client certificates

set -e

CERT_DIR="/tmp/synthos_certs"
mkdir -p "$CERT_DIR"

echo "======================================"
echo "Generating mTLS Certificates"
echo "======================================"

# 1. Generate CA (Certificate Authority)
echo ""
echo "Step 1: Generating CA certificate..."
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
    -keyout "$CERT_DIR/ca.key" \
    -out "$CERT_DIR/ca.crt" \
    -subj "/C=US/ST=CA/L=SF/O=Synthos/OU=ML/CN=Synthos-CA"

# 2. Generate Server Certificate
echo ""
echo "Step 2: Generating server certificate..."
openssl req -newkey rsa:4096 -nodes \
    -keyout "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.csr" \
    -subj "/C=US/ST=CA/L=SF/O=Synthos/OU=ML/CN=ml-service.synthos.local"

# Sign server certificate with CA
openssl x509 -req -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.crt" \
    -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERT_DIR/server.crt" \
    -days 365 \
    -sha256

# 3. Generate Client Certificate
echo ""
echo "Step 3: Generating client certificate..."
openssl req -newkey rsa:4096 -nodes \
    -keyout "$CERT_DIR/client.key" \
    -out "$CERT_DIR/client.csr" \
    -subj "/C=US/ST=CA/L=SF/O=Synthos/OU=Backend/CN=backend.synthos.local"

# Sign client certificate with CA
openssl x509 -req -in "$CERT_DIR/client.csr" \
    -CA "$CERT_DIR/ca.crt" \
    -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERT_DIR/client.crt" \
    -days 365 \
    -sha256

# 4. Cleanup CSR files
rm -f "$CERT_DIR/*.csr"

# 5. Set permissions
chmod 600 "$CERT_DIR/*.key"
chmod 644 "$CERT_DIR/*.crt"

echo ""
echo "======================================"
echo "Certificates generated successfully!"
echo "======================================"
echo ""
echo "Generated files in $CERT_DIR:"
ls -lh "$CERT_DIR"
echo ""
echo "CA Certificate:      $CERT_DIR/ca.crt"
echo "Server Certificate:  $CERT_DIR/server.crt"
echo "Server Key:          $CERT_DIR/server.key"
echo "Client Certificate:  $CERT_DIR/client.crt"
echo "Client Key:          $CERT_DIR/client.key"
echo ""
echo "Copy these to /etc/synthos/certs/ for production use:"
echo "  sudo mkdir -p /etc/synthos/certs"
echo "  sudo cp $CERT_DIR/* /etc/synthos/certs/"
echo ""
