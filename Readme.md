Welcome Data Engineering


# 1. Make sure you're in the right directory
ls -la  # Should show Dockerfile, docker-compose.yml, dags/, raw_api/

# 2. Stop everything
docker-compose down -v

# 3. Build all images
docker-compose build --no-cache

# 4. Start all services
docker-compose up -d

# 5. Check all services are running
docker-compose ps

# 6. Watch startup logs
docker-compose logs -f

# 7. Verify FastAPI is accessible
curl http://localhost:8000/health

# 8. Test from Airflow container
docker exec airflow-scheduler curl http://fastapi:8000/health

# 9. Check Airflow UI
# Open: http://localhost:8080
# Login: admin/admin

# 10. Trigger DAG
docker exec airflow-scheduler airflow dags trigger produce_json

# 11. Monitor DAG run
watch docker exec airflow-scheduler airflow dags list-runs -d produce_json


#!/bin/bash

echo "🔍 Testing Integration..."

# Test 1: FastAPI health
echo -n "1. FastAPI health: "
curl -s http://localhost:8000/health && echo "✅" || echo "❌"

# Test 2: Airflow health
echo -n "2. Airflow health: "
curl -s http://localhost:8080/health && echo "✅" || echo "❌"

# Test 3: Airflow to FastAPI connectivity
echo -n "3. Airflow -> FastAPI: "
docker exec airflow-scheduler curl -s http://fastapi:8000/health && echo "✅" || echo "❌"

# Test 4: Test API endpoint
echo -n "4. Test API endpoint: "
docker exec airflow-scheduler curl -s http://fastapi:8000/users | head -c 100 && echo "... ✅" || echo "❌"

# Test 5: Check DAG status
echo -n "5. DAG status: "
docker exec airflow-scheduler airflow dags list | grep produce_json && echo "✅" || echo "❌"

echo ""
echo "If all tests pass ✅, your integration is complete!"
echo "Trigger DAG: docker exec airflow-scheduler airflow dags trigger produce_json"


chmod +x test_integration.sh
./test_integration.sh
