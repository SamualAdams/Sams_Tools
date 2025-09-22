# Demand Planning Agent - Knowledge Base

This file serves as the central knowledge base index for the Demand Planning Agent project. All guides, documentation, and resources are organized here for easy reference by developers and AI agents.

## Project Overview

The Demand Planning Agent is built on a Unix-like tool composition philosophy where small, focused tools are combined to create powerful data processing workflows. The system uses PySpark with Delta Lake for distributed data processing and follows medallion architecture patterns.

## Knowledge Base Structure

### 📋 Guides
- **[Tooling Guide](resources/guide__tooling.md)** - Comprehensive guide to tool philosophy, composition patterns, and usage examples
  - Unix-like tool principles
  - Core tools documentation
  - Workflow composition patterns
  - Agent guidelines and best practices

- **[Demand Planning Game Guide](resources/guide__demand_planning_game.md)** - Complete game documentation and strategy guide
  - Game mechanics and scoring system
  - Monthly cycle workflow (analyze → forecast → results → learn)
  - Customer-specific strategies and forecasting approaches
  - Scenario handling and advanced techniques
  - Performance benchmarks and success tips

- **[Game Runner Guide](GAME_RUNNER_GUIDE.md)** - Interactive game runner and performance monitoring
  - Command-line interface for running games with real-time monitoring
  - Learning progression tracking and visualization across multiple games
  - Performance analytics and strategy effectiveness analysis
  - Multi-agent training and comparison capabilities

### 🔧 Core Tools

#### Session Management
- **`tools/tool__workstation.py`** - Centralized Spark session orchestration
  - Singleton pattern for consistent session management
  - Multiple configuration presets (local_delta, local_basic, local_performance)
  - Health checking and session lifecycle management
  - Cross-environment compatibility (local Mac + Databricks)

#### Workflow Orchestration  
- **`tools/tool__dag_chainer.py`** - DataFrame workflow management and inspection
  - Chain DataFrames with semantic naming (`dag__<action>_<description>`)
  - Inspection methods (`.look()`, `.trace()`, `.pick()`)
  - Universal display compatibility (Databricks + local environments)
  - Delta Lake integration for persistence

#### Data Standardization
- **`tools/tool__table_polisher.py`** - Consistent DataFrame standardization
  - Column name normalization (lowercase, special chars → underscores)
  - Key column value standardization (keyP__/keyF__ handling)
  - Column reordering (key columns, code columns, others - alphabetical)
  - Medallion architecture compliance

#### LLM Integration
- **`tools/tool__local_llm.py`** - Local LLM client for agentic reasoning
  - Ollama integration with automatic availability detection
  - Mock response fallback for development without LLM
  - Demand planning specific prompt engineering
  - Structured response parsing with confidence scoring

#### Demand Analysis
- **`tools/tool__demand_analysis.py`** - Historical demand pattern analysis
  - Seasonal pattern detection and indexing
  - Trend analysis and customer segmentation
  - Demand shift detection with change point analysis
  - Comprehensive insights generation with LLM interpretation

#### Demand Prediction
- **`tools/tool__demand_prediction.py`** - Forecasting and scenario generation
  - Multi-method ensemble forecasting (trend, seasonal, ensemble)
  - Confidence interval estimation and prediction quality scoring
  - What-if scenario generation (economic, seasonal, customer loss)
  - Strategic LLM guidance for forecasting approach

### 📊 Workflows

#### Current Implementations
- **`workflows/interactive_game_runner.ipynb`** - Interactive game runner with real-time monitoring
  - Run single games or training sessions with live progress updates
  - Rich formatting with color-coded performance indicators
  - Immediate analysis and visualization after each game
  - Easy configuration and experimentation interface
  - Export capabilities for detailed external analysis

- **`workflows/workflow__exmple_demand_pipeline.ipynb`** - Simple ingestion pipeline
  - CSV import → standardization → gold layer
  - Demonstrates tool composition patterns
  - Clean, minimal output for production use

- **`workflows/sandbox.ipynb`** - Development sandbox for experimentation

### 🤖 Agents
- **`agents/simple_forecast_agent.py`** - Forecast accuracy analysis agent
  - Proves business value with actionable insights
  - Automated MAPE analysis and trend detection
  - Structured reporting with severity levels and recommendations

- **`agents/agent_memory.py`** - Persistent learning and memory system
  - SQLite database for strategy performance tracking
  - Customer behavior profiles with learned characteristics
  - Scenario response optimization and cross-game knowledge
  - Learning analytics and performance insights

- **`agents/adaptive_forecast_agent.py`** - Learning-enabled forecasting agent
  - Uses memory system to select optimal strategies
  - Adapts forecasting methods based on historical performance
  - Balances exploration vs exploitation in strategy selection
  - Continuously improves through practice and feedback

### 🎮 Game Engines  
- **`engines/game_engine.py`** - Complete demand planning simulation game
  - 12-month simulation cycle management
  - Monthly forecast → results → feedback → learning loop
  - Multi-dimensional performance scoring (accuracy, consistency, learning, scenarios)
  - Scenario injection capabilities and agent adaptation tracking
  - Save/load game state and comprehensive final analysis

### 🗂️ Environment & Setup

#### Configuration
- **`env.md`** - Complete environment setup guide
  - Python 3.10 + PySpark + Delta Lake configuration
  - Jupyter kernel setup ("Demand Planning Agent")
  - Troubleshooting guide for common issues
  - Ready-to-work checklist

#### Dependencies
- **`requirements.txt`** - Python package dependencies
  - PySpark 3.5.1
  - Delta Spark 3.2.0
  - JupyterLab + ipykernel
  - Databricks Labs DQX

### 📁 Data Resources

#### Sample Data
- **`resources/data__example_demand.csv`** - Original example demand planning data
  - Customer codes, dates, actual/forecast values
  - Used for basic testing and examples
- **`resources/data__realistic_demand.csv`** - Realistic 12-month customer demand data
  - 5 customers with different volatility and volume profiles
  - Complete 2024 historical data for game simulation
  - Designed for testing analysis and prediction tools

#### LLM Prompts
- **`prompts/demand_insights.md`** - Demand pattern analysis prompt template
  - Structured framework for demand intelligence analysis
  - Customer segmentation and business impact focus
  - Pattern recognition and forecasting recommendations
- **`prompts/forecast_analysis.md`** - Forecast accuracy analysis prompt template
  - Performance assessment and root cause analysis
  - Structured reporting format with actionable insights
  - MAPE thresholds and business impact prioritization

#### Output Locations
- **`resources/gold/`** - Gold layer Delta tables (production-ready data)
- **`resources/delta_output/`** - General Delta table outputs

## Usage Patterns

### Basic Tool Composition
```python
from tools.tool__workstation import get_spark
from tools.tool__dag_chainer import DagChain  
from tools.tool__table_polisher import polish

# Initialize
spark = get_spark("local_delta")
chain__gold_demand = DagChain()

# Compose: Import → Standardize → Chain → Persist
chain__gold_demand.dag__import_csv = polish(
    spark.read.option("header", "true").csv("data.csv")
)
chain__gold_demand.write_to_path("resources/gold/demand_data", 0)
```

### Workflow Inspection
```python
# View all DataFrames in chain
chain__gold_demand.trace(shape=True)

# Inspect specific DataFrame
chain__gold_demand.look(0)  # First DataFrame
chain__gold_demand.look(-1) # Latest DataFrame
```

## Naming Conventions

### Tools
- Pattern: `tool__<purpose>`
- Examples: `tool__workstation`, `tool__dag_chainer`, `tool__table_polisher`

### Workflows  
- Pattern: `workflow__<domain>_<description>`
- Examples: `workflow__exmple_demand_pipeline`

### Chains
- Pattern: `chain__<layer>_<domain>`
- Examples: `chain__gold_demand`, `chain__silver_inventory`

### DAGs (within chains)
- Pattern: `dag__<action>_<description>`
- Examples: `dag__import_csv`, `dag__enrich_forecast`, `dag__aggregate_monthly`

### Files & Paths
- Data: `resources/<layer>/<domain>_<description>`
- Guides: `resources/guide__<topic>`
- Examples: `resources/gold/demand_data`, `resources/guide__tooling`

## Development Guidelines

### For Human Developers
1. **Start with session management** - Initialize workstation first
2. **Use chains for visibility** - Maintain workflow transparency
3. **Standardize early** - Apply table polisher at ingestion
4. **Name semantically** - Use descriptive names for all components
5. **Inspect frequently** - Verify transformations with `.look()` and `.trace()`

### For AI Agents
1. **Follow composition patterns** - Combine tools rather than creating monoliths
2. **Reference this knowledge base** - Use established patterns and conventions
3. **Maintain consistency** - Follow naming and structural conventions
4. **Document additions** - Update this file when adding new resources
5. **Test cross-environment** - Ensure compatibility with local and Databricks

## Development Status & Roadmap

### ✅ Completed
- **Core Infrastructure**
  - Unix-like tool composition architecture
  - Centralized Spark session management with presets
  - DataFrame workflow orchestration with inspection
  - Data standardization and medallion architecture compliance
  
- **Local LLM Integration**  
  - Ollama client with automatic availability detection
  - Mock response fallback for development
  - Demand planning specific prompt engineering
  - Structured response parsing with confidence scoring

- **Analysis Capabilities**
  - Historical demand pattern detection (seasonal, trends, volatility)
  - Customer segmentation by volume and volatility characteristics
  - Demand shift detection using change point analysis
  - Comprehensive insights generation with LLM interpretation

- **Prediction Capabilities**
  - Multi-method ensemble forecasting (trend-based, seasonal, ensemble)
  - Confidence interval estimation and prediction quality assessment
  - Scenario generation (economic downturn, seasonal surge, key customer loss)
  - Strategic LLM guidance for forecasting approach selection

- **Agent Learning System** 🧠
  - **Persistent Memory**: SQLite database storing strategy performance across games
  - **Customer Profiles**: Learned characteristics and optimal approaches per customer
  - **Scenario Learning**: Adaptive responses to market conditions and events
  - **Cross-Game Knowledge**: Cumulative intelligence building over multiple sessions
  - **Adaptive Agents**: Learning-enabled agents that improve through practice
  - **Performance Analytics**: Comprehensive learning progress tracking and insights

### 🚧 In Progress  
- **Advanced Learning Features** - Enhanced memory analytics and strategy optimization
- **Multi-Agent Scenarios** - Multiple agents competing and collaborating

### 📋 Next Phase
- **Enhanced Game Scenarios** - More sophisticated market conditions and events
- **Advanced Learning Algorithms** - Reinforcement learning and meta-learning
- **Multi-Agent Environments** - Competitive and collaborative agent scenarios
- **Real-Time Learning** - Dynamic adaptation within game sessions

### Future Expansion Areas

#### Planned Capabilities
- [ ] **Advanced Forecasting Models** - Machine learning integration
- [ ] **Real-time Data Processing** - Streaming demand updates
- [ ] **Multi-horizon Planning** - Short, medium, long-term integrated planning
- [ ] **Supply Chain Integration** - Inventory and capacity planning
- [ ] **Market Intelligence** - External factor incorporation

#### Infrastructure Enhancements
- [ ] **Data Quality Framework** - Validation and profiling utilities
- [ ] **Pipeline Monitoring** - Observability and alerting
- [ ] **Deployment Automation** - Production deployment patterns
- [ ] **Security Framework** - Data governance and access controls

## Quick Reference

| Component | Location | Purpose |
|-----------|----------|---------|
| **Game Runner** | `run_game.py` | Interactive game runner with performance monitoring |
| **Performance Monitor** | `performance_monitor.py` | Learning analytics and strategy effectiveness analysis |
| **Game Runner Guide** | `GAME_RUNNER_GUIDE.md` | Complete guide to running and monitoring games |
| **Tooling Guide** | `resources/guide__tooling.md` | Complete tool documentation |
| **Environment Setup** | `env.md` | Development environment configuration |
| **Core Tools** | `tools/tool__*.py` | Reusable data processing utilities |
| **Agents** | `agents/*.py` | Intelligent decision-making agents |
| **Game Engines** | `engines/*.py` | Simulation and game management |
| **Workflows** | `workflows/*.ipynb` | End-to-end pipeline implementations |
| **LLM Prompts** | `prompts/*.md` | Structured prompts for local LLM |
| **Sample Data** | `resources/data__*` | Test datasets and examples |
| **Knowledge Base** | `CLAUDE.md` | This file - central documentation index |

---

**Note**: This knowledge base should be updated whenever new guides, tools, or patterns are added to maintain comprehensive documentation for all agents working on the project.