#!/bin/sh
#
# © Copyright IBM Corp. 2026
#

#
# Extracts bootstrap addresses and CA certificate PEM for a published virtual
# topic from the EEM Manager Admin REST API.
#
# Prerequisites:
#   - curl and jq must be installed and on PATH.
#   - An EEM Admin API URL and an access token are required.
#     To obtain them, open the EEM application and click your avatar (top-right),
#     then select Profile > Access tokens tab:
#       Admin API URL  — copy the URL shown under "Admin API URL".
#       Access token   — click "Create token", enter a description, then copy
#                        the generated token value.
#
# Usage:
#   eem-connection-info.sh -u <manager-url> -k <token> \
#                          -t <topic-name> -a <alias> -g <group> [-i] [-o <dir>]
#
#   -u  EEM Manager Admin API base URL        (obtain from the manager UI)
#   -k  Bearer token                          (obtain from the manager UI)
#   -t  Topic name in the EEM catalog
#   -a  Virtual topic alias
#   -g  Gateway group name
#   -i  Use internal endpoints (default: external)
#   -o  Output directory for output files (default: current directory)
#   -h  Show this help
#
# Output:
#   bootstrap-server.txt  written to <output-dir>/bootstrap-server.txt  (host:port,...)
#   ca.pem                written to <output-dir>/ca.pem
#   Kafka client properties also printed to stdout.
#
#   eem-connection-info.sh \
#     -u https://eem.example.com/admin \
#     -k <bearer-token> \
#     -t bids-consume \
#     -a bid-cons \
#     -g poc-group \
#     -i \
#     -o /tmp


set -e  # exit immediately on any command failure

# ── Prerequisites check ───────────────────────────────────────────────────────
for tool in curl jq; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "Error: '$tool' is required but not found on PATH. Please install it and try again." >&2
        exit 1
    fi
done

# ── Defaults ──────────────────────────────────────────────────────────────────
MANAGER_URL=""
TOKEN=""
FILE=""
TOPIC=""
ALIAS=""
GROUP=""
ENDPOINT_TYPE="endpoints"
OUTPUT_DIR="."

# ── Argument parsing ──────────────────────────────────────────────────────────
usage() {
    # extract the Usage block from this file's own header comment and strip leading "# "
    sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
}

while getopts "u:k:f:t:a:g:io:h" opt; do
    case "$opt" in
        u) MANAGER_URL="$OPTARG" ;;
        k) TOKEN="$OPTARG" ;;
        f) FILE="$OPTARG" ;;
        t) TOPIC="$OPTARG" ;;
        a) ALIAS="$OPTARG" ;;
        g) GROUP="$OPTARG" ;;
        i) ENDPOINT_TYPE="internalEndpoints" ;;  # selects internalEndpoints[] instead of endpoints[] in the API response
        o) OUTPUT_DIR="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# ── Validate required arguments ───────────────────────────────────────────────
if [ -z "$TOPIC" ] || [ -z "$ALIAS" ] || [ -z "$GROUP" ]; then
    echo "Error: -t, -a, and -g are required." >&2
    usage
fi

if [ -z "$MANAGER_URL" ] && [ -z "$FILE" ]; then
    echo "Error: supply either -u <manager-url> and -k <token>, or -f <json-file>." >&2
    usage
fi

if [ -n "$MANAGER_URL" ] && [ -z "$TOKEN" ]; then
    echo "Error: -k <token> is required when -u is set." >&2
    usage
fi

# ── Fetch or read JSON ────────────────────────────────────────────────────────
if [ -n "$MANAGER_URL" ]; then
    ENDPOINT="${MANAGER_URL}/eventendpoints"
    HTTP_BODY=$(mktemp)  # temp file to capture response body separately from the status code
    # -o writes body to file, -w prints only the HTTP status code to stdout
    # || true prevents set -e from exiting if curl itself fails (e.g. DNS error returns exit code 6)
    HTTP_CODE=$(curl -k -s -o "$HTTP_BODY" -w "%{http_code}" \
        -H "Authorization: Bearer ${TOKEN}" \
        "$ENDPOINT") || true
    # curl returns "000" when it cannot reach the server at all (DNS failure, connection refused, timeout)
    if [ -z "$HTTP_CODE" ] || [ "$HTTP_CODE" = "000" ]; then
        rm -f "$HTTP_BODY"
        echo "Error: could not connect to ${ENDPOINT}" >&2
        echo "       Check the URL and that the manager is reachable." >&2
        exit 1
    fi
    if [ "$HTTP_CODE" -lt 200 ] || [ "$HTTP_CODE" -ge 300 ]; then
        BODY=$(cat "$HTTP_BODY"); rm -f "$HTTP_BODY"
        echo "Error: manager returned HTTP ${HTTP_CODE} from ${ENDPOINT}" >&2
        [ -n "$BODY" ] && echo "       Response: ${BODY}" >&2
        exit 1
    fi
    JSON=$(cat "$HTTP_BODY"); rm -f "$HTTP_BODY"
    if [ -z "$JSON" ]; then
        echo "Error: empty response from ${ENDPOINT}" >&2
        exit 1
    fi
    if ! printf '%s' "$JSON" | jq empty 2>/dev/null; then
        echo "Error: response from manager is not valid JSON:" >&2
        printf '%s\n' "$JSON" | head -5 >&2
        exit 1
    fi
elif [ "$FILE" = "-" ]; then
    JSON=$(cat)
else
    if [ ! -f "$FILE" ]; then
        echo "Error: file not found: $FILE" >&2
        exit 1
    fi
    JSON=$(cat "$FILE")
fi

# ── Locate the event endpoint ─────────────────────────────────────────────────
# jq streams all matching objects as concatenated JSON; a unique topic name produces exactly one
EP=$(printf '%s' "$JSON" | jq -r \
    --arg topic "$TOPIC" \
    '.[] | select(.name == $topic)')

if [ -z "$EP" ]; then
    echo "Error: no event endpoint found with topic name '$TOPIC'" >&2
    exit 1
fi

# ── Locate the virtual topic option ──────────────────────────────────────────
OPT=$(printf '%s' "$EP" | jq -r \
    --arg alias "$ALIAS" \
    '.options[] | select(.alias == $alias)')

if [ -z "$OPT" ]; then
    echo "Error: no virtual topic with alias '$ALIAS' under topic '$TOPIC'" >&2
    exit 1
fi

# ── Locate the gateway group ──────────────────────────────────────────────────
GW=$(printf '%s' "$OPT" | jq -r \
    --arg group "$GROUP" \
    '.gatewaysPublishedTo[] | select(.group == $group)')

if [ -z "$GW" ]; then
    echo "Error: no gateway group '$GROUP' for alias '$ALIAS'" >&2
    exit 1
fi

# ── Extract entries for the chosen endpoint type ──────────────────────────────
# .[$type] uses the shell variable as a JSON key name to select endpoints[] or internalEndpoints[]
ENTRIES=$(printf '%s' "$GW" | jq -r \
    --arg type "$ENDPOINT_TYPE" \
    '.[$type]')

COUNT=$(printf '%s' "$ENTRIES" | jq 'length')
if [ "$COUNT" -eq 0 ]; then
    echo "Error: '$ENDPOINT_TYPE' is empty for group '$GROUP'" >&2
    exit 1
fi

# ── Build bootstrap.servers ───────────────────────────────────────────────────
BOOTSTRAP=$(printf '%s' "$ENTRIES" | jq -r \
    '[.[] | .host + ":" + (.port | tostring)] | join(",")')

# ── Extract and deduplicate CA PEM(s) ─────────────────────────────────────────
# All brokers in a gateway group share the same CA issuer; unique[] collapses identical PEM blocks
# into one so ca.pem contains a single certificate regardless of how many gateways are in the group
CA_PEM=$(printf '%s' "$ENTRIES" | jq -r \
    '[.[].certificates[].pem] | unique | .[]')

if [ -z "$CA_PEM" ]; then
    echo "Error: no CA certificates found in '$ENDPOINT_TYPE' for group '$GROUP'" >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR"
CA_FILE="${OUTPUT_DIR}/ca.pem"
printf '%s\n' "$CA_PEM" > "$CA_FILE"  # printf preserves embedded newlines in PEM blocks; echo would not on all platforms
BOOTSTRAP_FILE="${OUTPUT_DIR}/bootstrap-server.txt"
printf '%s\n' "$BOOTSTRAP" > "$BOOTSTRAP_FILE"

# ── Output ────────────────────────────────────────────────────────────────────
echo ""
echo "--- Virtual topic server URLs ---"
echo "Bootstrap servers written to: ${BOOTSTRAP_FILE}"
echo ""
echo "--- CA certificates ---"
echo "CA certificate written to: ${CA_FILE}"
echo ""
echo "--- Kafka client properties ---"
echo "bootstrap.servers=${BOOTSTRAP}"
echo "ssl.truststore.location=${CA_FILE}"
echo ""
