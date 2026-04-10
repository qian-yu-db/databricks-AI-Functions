#!/bin/bash

# Databricks Asset Bundle Workflow Runner for IE Selected Pages Workflow
# Usage: ./run_workflow.sh [--profile PROFILE] [--target TARGET] [OPTIONS]

set -e  # Exit on any error

# Default values
PROFILE=""
TARGET="dev"
SKIP_VALIDATION=false
SKIP_DEPLOYMENT=false
JOB_ID=""
VAR_ARGS=()

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}INFO: $1${NC}"
}

print_success() {
    echo -e "${GREEN}SUCCESS: $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

OPTIONS:
    --profile PROFILE          Databricks profile to use for authentication
    --target TARGET            Deployment target (dev or prod, default: dev)
    --skip-validation          Skip bundle validation step
    --skip-deployment          Skip bundle deployment step
    --job-id JOB_ID           Job ID to run (skip deployment and use existing job)
    --var KEY=VALUE           Override DAB variable (can be used multiple times)
    --help                     Show this help message

EXAMPLES:
    # Basic usage with defaults
    $0

    # Use specific profile and prod target
    $0 --profile my-profile --target prod

    # Run specific job ID directly
    $0 --job-id 123456 --profile my-profile

    # Skip validation and deployment (use existing deployment)
    $0 --skip-validation --skip-deployment --profile my-profile

    # Override DAB variables on the fly
    $0 --var clean_pipeline_tables=Yes
    $0 --var clean_pipeline_tables=Yes --var catalog=my_catalog

WORKFLOW TASKS:
    1. clean_pipeline_tables              - Clean/reset pipeline tables
    2. parse_documents                    - Parse PDF documents using ai_parse_document
    3. extract_page_content               - Extract content aggregated by page
    4. classify_pages                     - Classify pages using ai_classify (binary)
    5. extract_info_from_classified_pages - Extract key info from target pages

PIPELINE EXECUTION:
    1. Bundle validation (optional, skippable with --skip-validation)
    2. Bundle deployment (optional, skippable with --skip-deployment)
    3. Job execution (all tasks run in dependency order)

NOTES:
    - PDF files should be placed in Unity Catalog volume before running
    - Default volume path: /Volumes/fins_genai/unstructured_documents/ie_selected_pages_workflow/inputs/
    - Customize config.yaml to modify classification and extraction prompts

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        --skip-validation)
            SKIP_VALIDATION=true
            shift
            ;;
        --skip-deployment)
            SKIP_DEPLOYMENT=true
            shift
            ;;
        --job-id)
            JOB_ID="$2"
            shift 2
            ;;
        --var)
            VAR_ARGS+=("--var" "$2")
            shift 2
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate target
if [[ "$TARGET" != "dev" && "$TARGET" != "prod" ]]; then
    print_error "Invalid target: $TARGET. Must be 'dev' or 'prod'"
    exit 1
fi

# Build profile argument
PROFILE_ARG=""
if [[ -n "$PROFILE" ]]; then
    PROFILE_ARG="--profile $PROFILE"
fi

# Define catalog/schema (these match the defaults in databricks.yml)
CATALOG="fins_genai"
SCHEMA="unstructured_documents"

print_info "==================================================================="
print_info "  IE Selected Pages Workflow"
print_info "==================================================================="
print_info "Configuration:"
print_info "  Profile: ${PROFILE:-default}"
print_info "  Target: $TARGET"
print_info "  Catalog: $CATALOG"
print_info "  Schema: $SCHEMA"

# Show variable overrides if any
if [[ ${#VAR_ARGS[@]} -gt 0 ]]; then
    print_info "  Variable overrides:"
    for ((i=1; i<${#VAR_ARGS[@]}; i+=2)); do
        print_info "    - ${VAR_ARGS[$i]}"
    done
fi

print_info "==================================================================="

# Step 1: Validate bundle (unless skipped or using existing job)
if [[ "$SKIP_VALIDATION" == false && -z "$JOB_ID" ]]; then
    print_info ""
    print_info "Step 1: Validating Databricks asset bundle..."
    if databricks bundle validate $PROFILE_ARG "${VAR_ARGS[@]}"; then
        print_success "Bundle validation completed successfully!"
    else
        print_error "Bundle validation failed!"
        exit 1
    fi
else
    print_warning "Skipping bundle validation"
fi

# Step 2: Deploy bundle (unless skipped or using existing job)
if [[ "$SKIP_DEPLOYMENT" == false && -z "$JOB_ID" ]]; then
    print_info ""
    print_info "Step 2: Deploying Databricks asset bundle to '$TARGET' target..."
    if databricks bundle deploy --target $TARGET $PROFILE_ARG "${VAR_ARGS[@]}"; then
        print_success "Bundle deployed successfully to '$TARGET' target!"
    else
        print_error "Bundle deployment failed!"
        exit 1
    fi
else
    print_warning "Skipping bundle deployment"
fi

# Step 3: Run the workflow
print_info ""
print_info "Step 3: Running the workflow..."

if [[ -n "$JOB_ID" ]]; then
    # Use provided job ID
    print_info "Using provided job ID: $JOB_ID"

    if databricks jobs run-now --job-id $JOB_ID $PROFILE_ARG; then
        print_success "Workflow launched successfully!"
    else
        print_error "Failed to launch workflow!"
        exit 1
    fi
else
    # Use bundle run command
    print_info "Launching workflow via bundle..."
    print_info "Job name: ie_selected_pages_workflow"

    if databricks bundle run ie_selected_pages_workflow --target $TARGET $PROFILE_ARG "${VAR_ARGS[@]}"; then
        print_success "Workflow completed successfully!"
    else
        print_error "Workflow execution failed!"
        exit 1
    fi
fi

print_info ""
print_info "==================================================================="
print_success "Workflow execution completed successfully!"
print_info "==================================================================="
print_info ""
print_info "View results in Databricks:"
print_info "  Catalog: $CATALOG"
print_info "  Schema: $SCHEMA"
print_info "  Tables:"
print_info "    - ie_parsed_documents_raw (raw parsed documents)"
print_info "    - ie_page_content (page-level content)"
print_info "    - ie_classified_pages (classified pages)"
print_info "    - ie_extracted_info (extracted information)"
print_info "==================================================================="
