#!/bin/bash

# Function to try sending data to a cluster by testing its ports until the leader accepts it
send_data() {
    local payload=$1
    shift
    local ports=("$@")
    
    for port in "${ports[@]}"; do
        echo "Trying port $port..."
        # Capture both the body and the HTTP status code
        response=$(curl -s -w "\n%{http_code}" -X POST http://localhost:$port/data -H "Content-Type: application/json" -d "{\"payload\": \"$payload\"}")
        
        # Extract status code (last line) and body (everything else)
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | sed '$d')
        
        if [ "$http_code" == "200" ]; then
            echo "Success! Leader accepted the data: $body"
            return 0
        else
            # Extract the leader_hint from the JSON using grep/sed
            leader_hint=$(echo "$body" | grep -o '"leader_hint": "[^"]*"' | cut -d'"' -f4)
            echo "Node on port $port is a follower. It returned leader hint: $leader_hint"
        fi
    done
    echo "Failed to find the leader."
    return 1
}

echo "=== Sending data to Leaf Cluster A ==="
send_data "sensor_A_temp=22.5" 8081 8082 8083
echo -e "\n"

echo "=== Sending data to Leaf Cluster B ==="
send_data "sensor_B_temp=19.2" 8091 8092 8093
echo -e "\n"

echo "Done! Check 'docker compose logs -f' to see the data propagate."
