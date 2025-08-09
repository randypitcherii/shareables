#!/bin/bash

# ShadowTraffic EC2 Deployment Script
# This script sources environment variables and deploys the ShadowTraffic EC2 instance

set -e  # Exit on any error

echo "🚀 Starting ShadowTraffic EC2 deployment..."

# Check if we're in the right directory
if [ ! -f "main.tf" ]; then
    echo "❌ Error: main.tf not found. Please run this script from the terraform directory."
    exit 1
fi

# Check if SSH key exists
if [ ! -f "$HOME/.ssh/msk-bastion-key.pem" ]; then
    echo "❌ Error: SSH key $HOME/.ssh/msk-bastion-key.pem not found."
    exit 1
fi

# Check if license.env exists
if [ ! -f "../license.env" ]; then
    echo "❌ Error: license.env not found in parent directory."
    exit 1
fi

# Check if cdc_generator.json exists
if [ ! -f "../cdc_generator.json" ]; then
    echo "❌ Error: cdc_generator.json not found in parent directory."
    exit 1
fi

echo "✅ Prerequisites check passed"

# Source environment variables from .env file if it exists
# From terraform dir -> up three levels to kafka_cdc_simulation/.env
ENV_FILE="../../../.env"
if [ -f "$ENV_FILE" ]; then
    echo "📄 Sourcing environment variables from $ENV_FILE"
    # shellcheck disable=SC1090
    source "$ENV_FILE"
else
    echo "⚠️  Warning: $ENV_FILE not found. You'll need to set environment variables manually."
    echo "Required variables: USERNAME, PASSWORD, KAFKA_BROKERS"
fi

# Set Terraform variables
export TF_VAR_username="${USERNAME:-}"
export TF_VAR_password="${PASSWORD:-}"
export TF_VAR_kafka_brokers="${KAFKA_BROKERS:-}"

# Validate required variables
if [ -z "$TF_VAR_username" ] || [ -z "$TF_VAR_password" ] || [ -z "$TF_VAR_kafka_brokers" ]; then
    echo "❌ Error: Missing required environment variables."
    echo "Please set USERNAME, PASSWORD, and KAFKA_BROKERS in your environment or .env file."
    exit 1
fi

echo "✅ Environment variables configured"

# Initialize Terraform
echo "🔧 Initializing Terraform..."
terraform init

# Plan the deployment
echo "📋 Planning deployment..."
terraform plan

# Ask for confirmation
echo ""
read -p "🤔 Do you want to proceed with the deployment? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Deployment cancelled."
    exit 1
fi

# Apply the deployment
echo "🚀 Applying Terraform configuration..."
terraform apply -auto-approve

echo ""
echo "✅ Deployment completed successfully!"

echo "📊 Deployment Summary:"
terraform output

echo ""
echo "🔍 To monitor the deployment:"
echo "  SSH to instance: $(terraform output -raw ssh_command)"
echo "  Check Docker status: $(terraform output -raw docker_status_command)"
echo "  View logs: $(terraform output -raw docker_logs_command)"

echo ""
echo "💡 To destroy the deployment: terraform destroy" 