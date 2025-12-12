# Agent Report Scraper API

A simplified Python web scraping service for agent reports with Flask API and MongoDB storage. Designed for external software integration.

## Features

- **Browser Automation**: Playwright-based scraping with robust error handling
- **MongoDB Integration**: Automatic data storage with optimized indexing
- **RESTful API**: Essential endpoints for scraping and data retrieval
- **External Integration**: Optimized for use by external software systems
- **Production Ready**: Simplified deployment without unnecessary features

## Quick Setup

### 1. Install Python 3.12+ (Recommended)

Download from: https://www.python.org/downloads/

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup MongoDB

Choose one option:

- **Local**: Install MongoDB Community Server
- **Cloud**: Use MongoDB Atlas (recommended for production)

### 4. Configure Environment

Create a `.env` file:

```env
# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE=agent_reports

# Scraper Authentication (if required)
SCRAPER_USERNAME=your_username
SCRAPER_PASSWORD=your_password

# Flask Configuration
FLASK_ENV=production
PORT=10000
```

### 5. Initialize Playwright

```bash
playwright install chromium
```

## API Endpoints

### Core Scraping
- `POST /api/scrape` - Start scraping task
- `GET /api/results/{task_id}` - Get task status and results

### MongoDB Data Access
- `GET /api/mongodb/agents` - Get all agents
- `GET /api/mongodb/agents/{agent_name}` - Get specific agent
- `GET /api/mongodb/reports` - Get recent reports
- `GET /api/mongodb/reports/{task_id}` - Get report by task ID
- `GET /api/mongodb/agents/performance` - Get agents by performance
- `GET /api/mongodb/statistics` - Get system statistics

### Monthly Organization & Filtering
- `GET /api/mongodb/months` - Get all available months with reports
- `GET /api/mongodb/reports/{year}/{month}` - Get reports for specific month
- `GET /api/mongodb/agents/{year}/{month}` - Get agents for specific month
- `GET /api/mongodb/agents/{agent_name}/{year}/{month}` - Get specific agent for specific month
- `GET /api/mongodb/statistics/{year}/{month}` - Get statistics for specific month

## Usage Example

### Start Scraping
```bash
curl -X POST http://localhost:10000/api/scrape \
  -H "Content-Type: application/json" \
  -d '{"username": "your_user", "password": "your_pass"}'
```

Response:
```json
{
  "success": true,
  "task_id": "task_20240101_120000",
  "message": "Scraping started successfully",
  "status_url": "/api/results/task_20240101_120000"
}
```

### Check Status
```bash
curl http://localhost:10000/api/results/task_20240101_120000
```

### Get All Agents
```bash
curl http://localhost:10000/api/mongodb/agents
```

### Monthly Data Examples

#### Get Available Months
```bash
curl http://localhost:10000/api/mongodb/months
```

Response:
```json
{
  "success": true,
  "months": [
    {
      "year": 2024,
      "month": 8,
      "month_name": "August",
      "month_year": "2024-08",
      "report_count": 15
    }
  ]
}
```

#### Get Agents for Specific Month
```bash
curl http://localhost:10000/api/mongodb/agents/2024/8
```

#### Get Specific Agent for Specific Month
```bash
curl http://localhost:10000/api/mongodb/agents/Ijeoma%20600/2024/8
```

Response:
```json
{
  "success": true,
  "year": 2024,
  "month": 8,
  "agent_name": "Ijeoma 600",
  "agent": {
    "agent_number": "600",
    "agent_name": "Ijeoma 600",
    "incoming_calls": {
      "total": "10",
      "unsuccessful": "0",
      "successful": "10",
      "min_time": "00:00:13",
      "max_time": "00:26:03",
      "avg_time": "00:05:19",
      "total_time": "00:53:15",
      "min_wait": "00:00:05",
      "max_wait": "00:00:18",
      "avg_wait": "00:00:09"
    },
    "outgoing_calls": {
      "total": "0",
      "unsuccessful": "0",
      "successful": "0",
      "min_time": "",
      "max_time": "",
      "avg_time": "00:00:00",
      "total_time": "00:00:00"
    },
    "year": 2024,
    "month": 8,
    "last_updated": "2024-08-15T10:30:00Z"
  }
}
```

#### Get Monthly Statistics
```bash
curl http://localhost:10000/api/mongodb/statistics/2024/8
```

Response:
```json
{
  "success": true,
  "statistics": {
    "year": 2024,
    "month": 8,
    "month_name": "August",
    "total_agents": 25,
    "total_reports": 15,
    "total_incoming_calls": 450,
    "total_outgoing_calls": 320,
    "total_calls": 770,
    "agents_with_calls": 23,
    "average_calls_per_agent": 33.48
  }
}
```

## Deployment

### Local Development
```bash
python app.py
```

### Production (Gunicorn)
```bash
gunicorn -w 4 -b 0.0.0.0:10000 app:app
```

### Docker
```bash
docker build -t agent-scraper .
docker run -p 10000:10000 agent-scraper
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `MONGODB_DATABASE` | Database name | `agent_reports` |
| `SCRAPER_USERNAME` | Authentication username | None |
| `SCRAPER_PASSWORD` | Authentication password | None |
| `PORT` | Server port | `10000` |
| `FLASK_ENV` | Flask environment | `production` |

## Data Structure

### Agent Document
```json
{
  "agent_name": "John Doe",
  "agent_number": "A001",
  "incoming_calls": {"total": 15, "details": "..."},
  "outgoing_calls": {"total": 8, "details": "..."},
  "actions": "Call details...",
  "task_id": "task_20240101_120000",
  "report_timestamp": "2024-01-01T12:00:00Z",
  "last_updated": "2024-01-01T12:30:00Z"
}
```

## Production Notes

- Uses MongoDB for persistent storage
- No local file storage required
- Optimized for external software integration
- Automatic data cleanup and indexing
- Error handling and logging built-in

## Support

For issues and questions, check the application logs and MongoDB connection status.