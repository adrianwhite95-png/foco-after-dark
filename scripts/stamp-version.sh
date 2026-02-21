#!/usr/bin/env bash
set -euo pipefail

VERSION="$(date -u +%Y.%m.%d).deploy-$(date -u +%H%M%S)"
cat > version.json <<EOF
{
  "version": "${VERSION}"
}
EOF

echo "Stamped version.json: ${VERSION}"
