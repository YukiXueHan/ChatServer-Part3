#!/bin/bash
SERVER_URL="ws://chatflow-alb-1154142688.us-west-2.elb.amazonaws.com"

for i in {1..60}; do
  echo ""
  echo "=========================================="
  echo "Run $i/60 - $(date)"
  echo "=========================================="

  java -jar target/client-part2-1.0-SNAPSHOT-jar-with-dependencies.jar $SERVER_URL

  if [ $i -lt 60 ]; then
    echo ""
    echo "Waiting 30 seconds for cleanup..."
    sleep 30  # Longer gap
  fi
done

echo ""
echo "Endurance test completed!"