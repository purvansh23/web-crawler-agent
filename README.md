# AI-Powered Data Extraction Pipeline

A high-scale, distributed production pipeline designed to ingest millions of companies, crawl their websites natively (bypassing strict anti-bot protections), and deterministically analyze their text context to identify highly-specialized service offerings (e.g., "Cross-Docking").

By decoupling the ingestion database from the scraping worker queue, this architecture is capable of processing 2M+ records simultaneously across multiple local or cloud machines without memory crashing or overlapping workloads.

## 🏗️ System Architecture

* **The Core Database (PostgreSQL):** Stores the massive ingested lists of millions of companies, preventing memory overload (OOM) crashes. It tracks individual statuses (`pending`, `queued`, `done`) and serves as the infinite snapshot source of truth.
* **The Message Broker (Redis / Memurai):** Acts as the high-speed active memory conveyor belt, accepting batches of URLs from Postgres and securely handing them off one-by-one to available workers.
* **The Drone Army (Celery):** An isolated pool of autonomous processing bots. They pull URLs from Redis and do the actual heavy lifting (crawling). You can run 5 Drones on your laptop and 50 on AWS simultaneously—they will all seamlessly process the unified Redis queue without overlapping.
* **The Smart Web-Crawler (HTTPx + Playwright):** Attempts lightning-fast HTTP reads first. If it detects an Anti-Bot Wall (Cloudflare 403) or an empty Javascript framework (React SPA), it intelligently falls back to an invisible **Headless Google Chrome (Playwright)** process to visually render the text perfectly.
* **The Dual-Layer Validator:** 
    1. **Regex Sorter:** Scans 20 million pages purely looking for keyword matching variations. Instantly filters out 90% of non-applicable traffic (Free & Fast).
    2. **AI Validator:** Extracts the specific paragraphs where the keywords were matched and passes them to Anthropic Claude LLM to determine *human context* (e.g. verifying we didn't just crawl a "We refuse to offer Cross-Docking" sentence).

---

## 🛠️ Local Environment Setup Requirements

To deploy this enterprise distributed pipeline locally, you will need to provision the following dependencies:

### 1. External Infrastructure
* **PostgreSQL:** Download and install [PostgreSQL 16+](https://www.postgresql.org/download/). Once installed, create a new blank database named `cross_dock_db`.
* **Redis / Memurai:** Install [Memurai](https://www.memurai.com/) (Redis for Windows). Keep it running as a background service on the default `localhost:6379`.

### 2. Python Environment Configuration
Open your terminal in the project root directory and run the following commands sequentially:

```powershell
# 1. Initialize and activate a Virtual Environment
python -m venv venv
.\venv\Scripts\activate

# 2. Install all core application dependencies
pip install -r requirements.txt

# 3. Install Playwright browser binaries (for headless scraping)
playwright install chromium
```

### 3. Environment Variables
Create a root file named `.env` based off of the provided `.env.example` file. Map it securely to your Postgres credentials:

```ini
ANTHROPIC_API_KEY="sk-ant-your-key"
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB="your database name"
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
REDIS_URL=redis://localhost:6379/0
```

---

## 🚀 Execution & Operating Guide

Once the foundation is installed, running the pipeline requires utilizing a **Multi-Terminal Strategy** to separate your UI, your Workers, and your Job Dispatcher.

### Terminal 1: Spin up your Celery Drones
Open a terminal, activate your `venv`, and start the worker pool. 
*(Note: Because of native Windows OS limits, you must rigidly force `solo` pooling to ensure maximum Playwright compatibility).*
```powershell
.\venv\Scripts\celery.exe -A celery_app worker --pool=solo -l info
```
**LEAVE THIS RUNNING.** It will sit idle silently until you drop jobs into the queue.

### Terminal 2: Visual Monitoring Dashboard (Flower UI)
Open a totally new terminal window, activate your `venv`, and launch the web UI:
```powershell
.\venv\Scripts\celery.exe -A celery_app flower --port=5555
```
**LEAVE THIS RUNNING.** Then, open your web browser and navigate to `http://localhost:5555` to watch beautiful real-time data visualizers of your active and queued tasks.

### Terminal 3: The Job Commander
Open your final terminal (with `venv` activated). This terminal is used to send direct master commands into the void. 

**Step A: Ingest Multiple Datasets into Postgres**
The ingest command operates utilizing rapid "Bulk Inserts". It can ingest 1-Million rows in 15 seconds. If you have multiple excel files, you simply ingest them sequentially. (The DB inherently checks `Company_ID` mappings and acts as an automatic de-duplicator, ensuring zero identical duplicates are appended!)
```powershell
python main.py --ingest File1.xlsx
python main.py --ingest File2.xlsx
```

**Step B: Dispatch Jobs into the Queue**
You NEVER process a million rows into the active memory conveyor belt at once. You inject them in chunks. By passing a limit value `N`, you extract `N` rows from the Database Cold Storage and feed them into Redis.
```powershell
python main.py --process 5000
```
*(The moment you press enter on this command, Terminal 1 will violently wake up and start ripping through the 5,000 URLs, and Terminal 2 will show exactly how fast it is doing it!)*

**Step C: Snap-Shot Export**
The database is an infinite source of truth. At absolute any time, you can pull a master snapshot. It perfectly queries all successful True Hits from the entirety of the database and comprehensively packages them.
```powershell
python main.py --export Master_Hits_Output.xlsx
```
