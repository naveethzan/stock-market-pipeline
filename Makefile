# Stock Market Streaming Pipeline
# Single Source of Truth: config/.env

.PHONY: help setup-dev setup-prod start-dev start-prod stop status deploy-connectors \
        dbt-setup dbt-run dbt-test clean logs docker-optimize

# ====================================
# DEFAULT HELP
# ====================================
help:
	@echo "📊 STOCK MARKET STREAMING PIPELINE"
	@echo "==================================="
	@echo ""
	@echo "🚀 QUICK START (ULTRA-FAST BUILDS):"
	@echo "  Development:  make setup-dev && make start-dev    # 2-3 min vs 30+ min!"
	@echo "  Production:   make setup-prod && make start-prod  # 2-3 min vs 30+ min!"
	@echo ""
	@echo "─────────────────────────────────────"
	@echo ""
	@echo "🧹 DOCKER OPTIMIZATION:"
	@echo "  docker-optimize - Clean Docker cache when builds get slow"
	@echo ""
	@echo "🔧 1. SETUP (ULTRA-FAST with Spark base caching):"
	@echo "  setup-dev    - LIGHTNING setup (2-3 min vs 30+ min) with mock data"
	@echo "  setup-prod   - LIGHTNING setup (2-3 min vs 30+ min) with real API"
	@echo ""
	@echo "⚡ 2. START:"
	@echo "  start-dev    - Start with MOCK data (overrides mock mode)"
	@echo "  start-prod   - Start with REAL API data"
	@echo ""
	@echo "🛑 3. STOP:"
	@echo "  stop         - Stop all services"
	@echo ""
	@echo "📊 4. STATUS:"
	@echo "  status       - Complete health check of all services"
	@echo ""
	@echo "🔄 5. CONNECTORS:"
	@echo "  deploy-connectors - Deploy all 3 connectors (Bronze/Silver/Redshift)"
	@echo ""
	@echo "📈 6. DBT (Optional):"
	@echo "  dbt-setup    - Initialize dbt for transformations"
	@echo "  dbt-run      - Run dbt transformations"
	@echo "  dbt-test     - Test data quality"
	@echo ""
	@echo "🔍 MONITORING:"
	@echo "  logs         - View streaming logs"
	@echo "  clean        - Clean all containers and data"
	@echo ""
	@echo "🌐 WEB INTERFACES:"
	@echo "  Spark Master: http://localhost:8080"
	@echo "  Kafka UI:     http://localhost:8090"
	@echo "  Producer:     http://localhost:8081/health"
	@echo "  Processor:    http://localhost:8082/health"
	@echo ""
	@echo "📝 NOTE: All configurations come from config/.env"

# ====================================
# 1. SETUP COMMANDS
# ====================================

# Development Setup
setup-dev:
	@echo "🔧 SETTING UP DEVELOPMENT ENVIRONMENT"
	@echo "======================================"
	@echo ""
	@echo "🔍 Checking config/.env..."
	@if [ ! -f config/.env ]; then \
		echo "❌ config/.env not found!"; \
		echo "Please create config/.env with your AWS and Redshift credentials"; \
		exit 1; \
	fi
	@echo "✅ Found config/.env"
	@echo ""
	@echo "📁 Creating directories..."
	@mkdir -p config src/spark/jobs data/spark logs/kafka-connect checkpoints
	@mkdir -p scripts
	@mkdir -p src/streaming_pipeline/dbt/stock_market_dbt/target
	@echo "✅ Directories created"
	@echo ""
	@echo "🐍 Installing Python dependencies..."
	@python3 -m venv venv 2>/dev/null || echo "Virtual environment exists"
	./venv/bin/pip install --upgrade pip
	if [ -f requirements-streaming.txt ] && [ -s requirements-streaming.txt ]; then \
		./venv/bin/pip install -r requirements-streaming.txt && \
		echo "✅ Streaming dependencies installed"; \
	fi
	@echo ""
	@echo "🔧 Making scripts executable..."
	@chmod +x scripts/*.sh 2>/dev/null || true
	@echo ""
	echo "🚀 Building Docker containers (ULTRA-FAST with Spark caching)..."
	./scripts/infrastructure/ultra-fast-build.sh
	@echo ""
	@echo "✅ DEVELOPMENT SETUP COMPLETE!" 
	@echo ""
	@echo "📝 Development mode will use:"
	@echo "   • config/.env for all credentials"
	@echo "   • ALPHA_VANTAGE_MOCK_MODE=true (override)"
	@echo "   • PRODUCTION_INTERVAL_SECONDS=60 (override)"
	@echo ""
	@echo "Next: make start-dev"

# Production Setup
setup-prod:
	@echo "🔧 SETTING UP PRODUCTION ENVIRONMENT"
	@echo "======================================"
	@echo ""
	@echo "🔍 Checking config/.env..."
	@if [ ! -f config/.env ]; then \
		echo "❌ config/.env not found!"; \
		echo "Please create config/.env with your AWS and Redshift credentials"; \
		exit 1; \
	fi
	@echo "✅ Found config/.env"
	@echo ""
	@echo "🔍 Validating credentials..."
	@if grep -q "YOUR_API_KEY_HERE\|YOUR_ACCESS_KEY\|YOUR_PASSWORD\|your-endpoint" config/.env; then \
		echo "⚠️  WARNING: Found placeholder values in config/.env"; \
		echo "Please update with real credentials before starting production"; \
	else \
		echo "✅ No placeholder values found"; \
	fi
	@echo ""
	@echo "📁 Creating directories..."
	@mkdir -p config src/spark/jobs data/spark logs/kafka-connect checkpoints
	@mkdir -p scripts
	@mkdir -p src/streaming_pipeline/dbt/stock_market_dbt/target
	@echo "✅ Directories created"
	@echo ""
	@echo "🐍 Installing Python dependencies..."
	@python3 -m venv venv 2>/dev/null || echo "Virtual environment exists"
	./venv/bin/pip install --upgrade pip
	if [ -f requirements-streaming.txt ] && [ -s requirements-streaming.txt ]; then \
		./venv/bin/pip install -r requirements-streaming.txt && \
		echo "✅ Streaming dependencies installed"; \
	fi
	@echo ""
	@echo "🔧 Making scripts executable..."
	@chmod +x scripts/*.sh 2>/dev/null || true
	@echo ""
	echo "🚀 Building Docker containers (ULTRA-FAST with Spark caching)..."
	./scripts/infrastructure/ultra-fast-build.sh
	@echo ""
	@echo "✅ PRODUCTION SETUP COMPLETE!"
	@echo ""
	@echo "📝 Production mode will use:"
	@echo "   • config/.env for all configurations"
	@echo "   • Real API data (as configured in .env)"
	@echo ""
	@echo "Next: make start-prod"

# ====================================
# 2. START COMMANDS
# ====================================

# Start Development Mode
start-dev:
	@echo "⚡ STARTING DEVELOPMENT MODE"
	@echo "============================"
	@echo ""
	@if [ ! -f config/.env ]; then \
		echo "❌ config/.env not found. Run: make setup-dev"; \
		exit 1; \
	fi
	@echo "🧹 Cleaning previous session..."
	@docker-compose -f docker/compose/docker-compose.yaml down --remove-orphans 2>/dev/null || true
	@docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml down --remove-orphans 2>/dev/null || true
	@rm -rf logs/*.log checkpoints/* 2>/dev/null || true
	@echo ""
	@echo "🚀 Starting services with MOCK data..."
	@echo "   Using config/.env with overrides:"
	@echo "   • ALPHA_VANTAGE_MOCK_MODE=true"
	@echo "   • PRODUCTION_INTERVAL_SECONDS=60"
	@echo ""
	@# Load config/.env and override for dev mode
	@set -a && \
	. config/.env && \
	export ALPHA_VANTAGE_MOCK_MODE=true && \
	export PRODUCTION_INTERVAL_SECONDS=60 && \
	export ENVIRONMENT=development && \
	set +a && \
	if [ -f scripts/infrastructure/start-cluster.sh ]; then \
		./scripts/infrastructure/start-cluster.sh; \
	else \
		docker-compose -f docker/compose/docker-compose.yaml up -d; \
	fi
	@echo ""
	@echo "⏳ Waiting for services to initialize (30 seconds)..."
	@sleep 30
	@echo ""
	@echo "✅ DEVELOPMENT MODE ACTIVE!"
	@echo ""
	@echo "📊 Services Running:"
	@echo "  • Spark Cluster: http://localhost:8080"
	@echo "  • Kafka UI: http://localhost:8090"
	@echo "  • Producer Health: http://localhost:8081/health"
	@echo "  • Processor Health: http://localhost:8082/health"
	@echo ""
	@echo "🚀 AUTO-DEPLOYING CONNECTORS..."
	@echo "=============================="
	@echo "🕰️ Waiting for all services to be fully healthy before deploying connectors..."
	@sleep 30
	@echo ""
	@echo "📊 Performing final health verification..."
	@# Quick health check before connector deployment
	@SERVICES_HEALTHY=true; \
	echo "Checking Producer..."; \
	if ! curl -f -s http://localhost:8081/health >/dev/null 2>&1; then \
		echo "❌ Producer not healthy"; SERVICES_HEALTHY=false; \
	else echo "✅ Producer healthy"; fi; \
	echo "Checking Kafka Connect..."; \
	if ! curl -f -s http://localhost:8083/connectors >/dev/null 2>&1; then \
		echo "❌ Kafka Connect not ready"; SERVICES_HEALTHY=false; \
	else echo "✅ Kafka Connect ready"; fi; \
	if [ "$$SERVICES_HEALTHY" = "true" ]; then \
		echo ""; \
		echo "🚀 All services healthy - deploying connectors automatically..."; \
		./scripts/connectors/scripts/deploy-connectors.sh || echo "⚠️  Some connectors may have failed - check status"; \
	else \
		echo "⚠️  Services not fully healthy - skipping auto-deployment"; \
		echo "Run 'make deploy-connectors' manually once services are healthy"; \
	fi
	@echo ""
	@echo "🎉 DEVELOPMENT PIPELINE READY!"
	@echo "============================="
	@echo ""
	@echo "📋 Next steps:"
	@echo "  1. make status            # Check complete health"
	@echo "  2. make logs              # Monitor pipeline"
	@echo "  3. make deploy-connectors # Re-deploy connectors if needed"

# Start Production Mode
start-prod:
	@echo "⚡ STARTING PRODUCTION MODE"
	@echo "==========================="
	@echo ""
	@if [ ! -f config/.env ]; then \
		echo "❌ config/.env not found. Run: make setup-prod"; \
		exit 1; \
	fi
	@if grep -q "YOUR_API_KEY_HERE\|YOUR_ACCESS_KEY\|YOUR_PASSWORD" config/.env; then \
		echo "❌ Please configure real credentials in config/.env"; \
		exit 1; \
	fi
	@echo "🧹 Cleaning previous session..."
	@docker-compose -f docker/compose/docker-compose.yaml down --remove-orphans 2>/dev/null || true
	@docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml down --remove-orphans 2>/dev/null || true
	@rm -rf logs/*.log checkpoints/* 2>/dev/null || true
	@echo ""
	@echo "🚀 Starting services with REAL API data..."
	@echo "   Using config/.env as configured"
	@echo ""
	@# Load config/.env directly
	@set -a && \
	. config/.env && \
	export ENVIRONMENT=production && \
	set +a && \
	if [ -f scripts/infrastructure/start-cluster.sh ]; then \
		./scripts/infrastructure/start-cluster.sh; \
	else \
		docker-compose -f docker/compose/docker-compose.yaml up -d; \
	fi
	@echo ""
	@echo "⏳ Waiting for services to initialize (30 seconds)..."
	@sleep 30
	@echo ""
	@echo "✅ PRODUCTION MODE ACTIVE!"
	@echo ""
	@echo "⚠️  Using real API with rate limits"
	@echo ""
	@echo "📋 Next steps:"
	@echo "  1. make status            # Check health"
	@echo "  2. make logs              # Monitor pipeline"
	@echo "  3. make deploy-connectors # Re-deploy connectors if needed"

# ====================================
# 3. STOP COMMAND
# ====================================

stop:
	@echo "🛑 STOPPING ALL SERVICES"
	@echo "========================"
	@docker-compose -f docker/compose/docker-compose.yaml down
	@if [ -f docker/compose/docker-compose.cluster.yml ]; then \
		docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml down; \
	fi
	@echo "✅ All services stopped"

# ====================================
# 4. STATUS COMMAND
# ====================================

status:
	@echo "📊 COMPLETE SYSTEM STATUS"
	@echo "========================="
	@echo ""
	@echo "🔍 Environment:"
	@if docker ps | grep -q streaming-producer; then \
		MODE=$$(docker exec streaming-producer printenv ALPHA_VANTAGE_MOCK_MODE 2>/dev/null || echo "unknown"); \
		if [ "$$MODE" = "true" ]; then \
			echo "  Mode: DEVELOPMENT (Mock Data)"; \
		else \
			echo "  Mode: PRODUCTION (Real API)"; \
		fi; \
	else \
		echo "  Mode: NOT RUNNING"; \
	fi
	@echo ""
	@echo "─────────────────────────────"
	@echo "1️⃣  SPARK CLUSTER"
	@echo "─────────────────────────────"
	@if docker ps | grep -q spark-master; then \
		echo "✅ Spark Master: Running"; \
		echo "   URL: http://localhost:8080"; \
	else \
		echo "❌ Spark Master: Not running"; \
	fi
	@if docker ps | grep -q spark-worker-1; then \
		echo "✅ Worker 1: Running (http://localhost:8181)"; \
	else \
		echo "❌ Worker 1: Not running"; \
	fi
	@if docker ps | grep -q spark-worker-2; then \
		echo "✅ Worker 2: Running (http://localhost:8182)"; \
	else \
		echo "❌ Worker 2: Not running"; \
	fi
	@echo ""
	@echo "─────────────────────────────"
	@echo "2️⃣  STREAMING SERVICES"
	@echo "─────────────────────────────"
	@echo -n "Producer: "
	@curl -s http://localhost:8081/health 2>/dev/null | \
		python3 -c "import sys, json; data = json.load(sys.stdin); print(f\"✅ {data.get('status', 'Unknown')} - {data.get('message', '')}\")" 2>/dev/null || \
		echo "❌ Not responding"
	@echo -n "Processor: "
	@curl -s http://localhost:8082/health 2>/dev/null | \
		python3 -c "import sys, json; data = json.load(sys.stdin); print(f\"✅ {data.get('status', 'Unknown')}\")" 2>/dev/null || \
		echo "❌ Not responding"
	@echo ""
	@echo "─────────────────────────────"
	@echo "3️⃣  KAFKA INFRASTRUCTURE"
	@echo "─────────────────────────────"
	@if docker ps | grep -q kafka; then \
		echo "✅ Kafka Broker: Running"; \
		echo "   Topics:" && \
		docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | head -5 | sed 's/^/     - /' || true; \
	else \
		echo "❌ Kafka: Not running"; \
	fi
	@if docker ps | grep -q schema-registry; then \
		echo "✅ Schema Registry: Running"; \
	else \
		echo "❌ Schema Registry: Not running"; \
	fi
	@if docker ps | grep -q kafka-connect; then \
		echo "✅ Kafka Connect: Running"; \
		echo -n "   Active Connectors: "; \
		curl -s http://localhost:8083/connectors 2>/dev/null | \
			python3 -c "import sys, json; c = json.load(sys.stdin); print(f'{len(c)} - {c if c else []}')" 2>/dev/null || echo "0"; \
	else \
		echo "❌ Kafka Connect: Not running"; \
	fi
	@if docker ps | grep -q kafka; then \
		echo "✅ Kafka UI: http://localhost:8090"; \
	fi
	@echo ""
	@echo "─────────────────────────────"
	@echo "4️⃣  DATA PIPELINE STATUS"
	@echo "─────────────────────────────"
	@echo "Checking data flow..."
	@echo -n "  Latest message: "
	@docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic stock-quotes-realtime \
		--max-messages 1 \
		--timeout-ms 5000 \
		--from-beginning 2>/dev/null | head -1 | cut -c1-60 || echo "No messages yet"
	@echo ""
	@echo "─────────────────────────────"
	@echo "5️⃣  RESOURCE USAGE"
	@echo "─────────────────────────────"
	@docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep -E "(NAME|kafka|spark|streaming)" || echo "No containers running"

# ====================================
# 5. DEPLOY CONNECTORS (ALL 3)
# ====================================

deploy-connectors:
	@echo "📊 DEPLOYING ALL KAFKA CONNECTORS"
	@echo "=================================="
	@echo ""
	@echo "🔍 Checking prerequisites..."
	@if [ ! -f config/.env ]; then \
		echo "❌ config/.env not found!"; \
		exit 1; \
	fi
	@if ! docker ps | grep -q kafka-connect; then \
		echo "❌ Kafka Connect not running. Start pipeline first."; \
		exit 1; \
	fi
	@echo "✅ Kafka Connect is running"
	@echo ""
	@echo "⏳ Waiting for Kafka Connect to be ready..."
	@for i in 1 2 3 4 5; do \
		if curl -s http://localhost:8083/ >/dev/null 2>&1; then \
			echo "✅ Kafka Connect is ready"; \
			break; \
		fi; \
		echo "  Waiting... ($$i/5)"; \
		sleep 5; \
	done
	@echo ""
	@echo "🚀 Deploying all connectors with enhanced Redshift support..."
	@# Use the unified deployment script
	@if [ -f scripts/connectors/scripts/deploy-connectors.sh ]; then \
		./scripts/connectors/scripts/deploy-connectors.sh; \
	else \
		echo "❌ deploy-connectors.sh not found!"; \
		exit 1; \
	fi
	@echo ""
	@echo "🔍 Verifying deployment..."
	@sleep 3
	@echo "Active connectors:"
	@curl -s http://localhost:8083/connectors 2>/dev/null | \
		python3 -c "import sys, json; connectors = json.load(sys.stdin); [print(f'  • {c}') for c in connectors]" 2>/dev/null || echo "  Unable to fetch"
	@echo ""
	@echo "✅ Connector deployment complete!"
	@echo ""
	@echo "📊 Data Pipeline Flow:"
	@echo "  1. Bronze: Raw Kafka → S3 (Avro format)"
	@echo "  2. Silver: Processed → S3 (Parquet format)"
	@echo "  3. Gold: Analytics → Redshift (Streaming tables)"

# ====================================
# 6. DBT COMMANDS (OPTIONAL)
# ====================================

dbt-setup:
	@echo "📊 SETTING UP DBT"
	@echo "================="
	@echo ""
	@echo "🐍 Installing dbt..."
	@./venv/bin/pip install dbt-core dbt-postgres dbt-redshift
	@echo "✅ dbt installed"
	@echo ""
	@echo "🔧 Initializing dbt project..."
	@cd src/streaming_pipeline/dbt/stock_market_dbt && \
	if [ -f dbt_project.yml ]; then \
		echo "✅ dbt project exists"; \
		if [ -f packages.yml ]; then \
			../../../../../venv/bin/dbt deps; \
		fi; \
	else \
		echo "Creating dbt project..."; \
		../../../../../venv/bin/dbt init stock_market_dbt --skip-profile-setup; \
	fi
	@echo ""
	@echo "✅ dbt setup complete"

dbt-run:
	@echo "📊 RUNNING DBT TRANSFORMATIONS"
	@echo "=============================="
	@cd src/streaming_pipeline/dbt/stock_market_dbt && \
	../../../../../venv/bin/dbt run --profiles-dir .
	@echo "✅ Transformations complete"

dbt-test:
	@echo "🧪 RUNNING DATA QUALITY TESTS"
	@echo "============================="
	@cd src/streaming_pipeline/dbt/stock_market_dbt && \
	../../../../../venv/bin/dbt test --profiles-dir .
	@echo "✅ Tests complete"

# ====================================
# MONITORING & CLEANUP
# ====================================

logs:
	@echo "📝 Streaming Pipeline Logs (Ctrl+C to exit)"
	@echo "==========================================="
	@docker-compose -f docker/compose/docker-compose.yaml logs -f streaming-producer streaming-processor

clean:
	@echo "🧹 CLEANING EVERYTHING"
	@echo "======================"
	@docker-compose -f docker/compose/docker-compose.yaml down -v --remove-orphans
	@if [ -f docker/compose/docker-compose.cluster.yml ]; then \
		docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml down -v; \
	fi
	@rm -rf logs/* checkpoints/* data/spark/temp/* 2>/dev/null || true
	@docker system prune -f
	@echo "✅ Cleanup complete"

# ====================================
# VERIFICATION (Hidden Helper)
# ====================================

.PHONY: verify
verify:
	@echo "🔍 Quick Pipeline Verification"
	@echo "=============================="
	@echo -n "Kafka Topics: "
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l | xargs echo
	@echo -n "Latest Message: "
	@docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic stock-quotes-realtime \
		--max-messages 1 \
		--timeout-ms 5000 \
		--from-beginning 2>/dev/null | head -1 | cut -c1-80 || echo "No messages"
	@echo -n "Connectors: "
	@curl -s http://localhost:8083/connectors 2>/dev/null | \
		python3 -c "import sys, json; c = json.load(sys.stdin); print(f'{len(c)} active')" 2>/dev/null || echo "0 active"
	@echo "✅ Verification complete"

# ====================================
# OPTIMIZED DOCKER BUILD COMMANDS
# ====================================

# Docker optimization and cleanup
docker-optimize:
	@echo "🧹 DOCKER OPTIMIZATION"
	@echo "======================="
	@echo ""
	@echo "📊 Current Docker usage:"
	@docker system df
	@echo ""
	@echo "🧹 Cleaning up unused Docker resources..."
	@docker system prune -f
	@docker builder prune -f
	@echo ""
	@echo "📊 After cleanup:"
	@docker system df
	@echo ""
	@echo "✅ Docker optimization complete!"

