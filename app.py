"""
Simplified Flask web application for the agent report scraper.
Provides only essential API endpoints for external software integration.
"""

import os
import asyncio
import json
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import threading
import pandas as pd

from scraper import AgentReportScraper
from mongodb_service import get_mongodb_service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=["*"], allow_headers=["Content-Type", "Authorization", "Accept"])

# Store scraper instances and results
scraper_results = {}

@app.route('/')
def home():
    """Health check endpoint."""
    return jsonify({
        "status": "running",
        "message": "Agent Report Scraper API with MongoDB Integration",
        "version": "3.1.0",
        "environment_check": {
            "has_username": bool(os.environ.get('SCRAPER_USERNAME')),
            "has_password": bool(os.environ.get('SCRAPER_PASSWORD')),
            "mongodb_configured": bool(os.environ.get('MONGODB_CONNECTION_STRING'))
        },
        "endpoints": {
            "scrape": {
                "url": "/api/scrape",
                "method": "POST or GET",
                "description": "Start a scraping task. Supports historical data scraping up to 12 months back.",
                "parameters": {
                    "year": "Target year (e.g., 2025) - optional, defaults to current year",
                    "month": "Target month (1-12) - optional, defaults to current month",
                    "username": "Login username - optional, uses env var if not provided",
                    "password": "Login password - optional, uses env var if not provided"
                },
                "example": "/api/scrape?year=2025&month=1"
            },
            "mongodb": {
                "all_agents": "/api/mongodb/agents",
                "agent_by_name": "/api/mongodb/agents/<agent_name>",
                "agent_by_name_and_month": "/api/mongodb/agents/<agent_name>/<year>/<month>",
                "recent_reports": "/api/mongodb/reports",
                "report_by_task_id": "/api/mongodb/reports/<task_id>",
                "agents_by_performance": "/api/mongodb/agents/performance",
                "statistics": "/api/mongodb/statistics",
                "monthly": {
                    "available_months": "/api/mongodb/months",
                    "reports_by_month": "/api/mongodb/reports/<year>/<month>",
                    "agents_by_month": "/api/mongodb/agents/<year>/<month>",
                    "monthly_statistics": "/api/mongodb/statistics/<year>/<month>"
                }
            }
        }
    })

@app.route('/api/scrape', methods=['POST', 'GET'])
def start_scraping():
    """Start a new scraping task."""
    try:
        logger.info(f"Scraping request received - Content-Type: {request.content_type}")
        logger.info(f"Request headers: {dict(request.headers)}")
        logger.info(f"Request is_json: {request.is_json}")
        logger.info(f"Request has form data: {bool(request.form)}")

        # Get optional parameters from request (handle both JSON and form data)
        data = {}
        username = None
        password = None

        # Try to get JSON data first, but don't fail if Content-Type is wrong
        try:
            if request.is_json:
                data = request.get_json() or {}
            elif request.form:
                data = request.form.to_dict()
        except Exception:
            # If JSON parsing fails, continue with empty data
            data = {}

        # Also check query parameters for GET requests
        if request.method == 'GET':
            data.update(request.args.to_dict())

        # Get target year and month (for scraping historical data)
        target_year = data.get('year')
        target_month = data.get('month')

        # Convert to int if provided
        if target_year:
            target_year = int(target_year)
        if target_month:
            target_month = int(target_month)

        # Generate unique task ID including the target month if specified
        if target_year and target_month:
            task_id = f"task_{target_year}{target_month:02d}_{datetime.now().strftime('%H%M%S')}"
            logger.info(f"Scraping historical data for {target_year}-{target_month:02d}")
        else:
            task_id = f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Get credentials from request data or use environment variables
        username = data.get('username') or os.environ.get('SCRAPER_USERNAME')
        password = data.get('password') or os.environ.get('SCRAPER_PASSWORD')

        logger.info(f"Using credentials - Username: {'***' if username else 'None'}, Password: {'***' if password else 'None'}")

        # Update auth config if credentials provided in request
        if data.get('username') and data.get('password'):
            os.environ['SCRAPER_USERNAME'] = data.get('username')
            os.environ['SCRAPER_PASSWORD'] = data.get('password')
            logger.info("Updated environment variables with request credentials")

        # Initialize scraper result
        scraper_results[task_id] = {
            "status": "starting",
            "created_at": datetime.now().isoformat(),
            "progress": 0,
            "message": "Initializing scraper...",
            "task_id": task_id
        }

        # Start scraping in background
        def run_scraper(year=None, month=None):
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                scraper_results[task_id]["status"] = "running"
                scraper_results[task_id]["progress"] = 10
                scraper_results[task_id]["message"] = "Browser starting..."
                if year and month:
                    scraper_results[task_id]["target_period"] = f"{year}-{month:02d}"

                scraper = AgentReportScraper(target_year=year, target_month=month)
                loop.run_until_complete(scraper.scrape())

                # Check if data was scraped successfully
                if scraper.scraped_data:
                    scraper_results[task_id]["status"] = "completed"
                    scraper_results[task_id]["progress"] = 100
                    scraper_results[task_id]["message"] = "Scraping completed successfully"
                    scraper_results[task_id]["data_count"] = len(scraper.scraped_data)
                    scraper_results[task_id]["scraped_data"] = scraper.scraped_data

                    # Save to MongoDB
                    try:
                        mongodb_service = get_mongodb_service()
                        document_id = mongodb_service.save_report(scraper.scraped_data, task_id, target_year=year, target_month=month)
                        scraper_results[task_id]["mongodb_id"] = document_id
                        period_info = f" for {year}-{month:02d}" if year and month else ""
                        scraper_results[task_id]["message"] += f" | Saved to MongoDB: {document_id}{period_info}"
                        logger.info(f"Scraping data saved to MongoDB: {document_id}{period_info}")
                    except Exception as mongo_error:
                        logger.error(f"MongoDB save failed: {mongo_error}")
                        scraper_results[task_id]["mongodb_error"] = str(mongo_error)

                else:
                    scraper_results[task_id]["status"] = "completed"
                    scraper_results[task_id]["progress"] = 100
                    scraper_results[task_id]["message"] = "Scraping completed but no data found"
                    scraper_results[task_id]["data_count"] = 0

            except Exception as e:
                scraper_results[task_id]["status"] = "error"
                scraper_results[task_id]["progress"] = 0
                scraper_results[task_id]["message"] = f"Scraping failed: {str(e)}"
                logger.error(f"Scraping error: {e}")
            finally:
                loop.close()

        # Start background task
        thread = threading.Thread(target=run_scraper, kwargs={'year': target_year, 'month': target_month})
        thread.daemon = True
        thread.start()

        response_data = {
            "success": True,
            "task_id": task_id,
            "message": "Scraping started successfully",
            "status_url": f"/api/results/{task_id}"
        }
        if target_year and target_month:
            response_data["target_period"] = f"{target_year}-{target_month:02d}"

        return jsonify(response_data), 202

    except Exception as e:
        logger.error(f"Error starting scraping: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "message": f"Failed to start scraping: {str(e)}"
        }), 500

@app.route('/api/results/<task_id>')
def get_task_results(task_id):
    """Get results for a specific task."""
    if task_id not in scraper_results:
        return jsonify({
            "success": False,
            "error": "Task not found",
            "task_id": task_id
        }), 404

    result = scraper_results[task_id]
    return jsonify({
        "success": True,
        "task_id": task_id,
        "status": result["status"],
        "progress": result["progress"],
        "message": result["message"],
        "created_at": result["created_at"],
        "data_count": result.get("data_count", 0),
        "mongodb_id": result.get("mongodb_id"),
        "scraped_data": result.get("scraped_data") if result["status"] == "completed" else None
    })

# MongoDB-specific endpoints
@app.route('/api/mongodb/agents')
def get_all_agents():
    """Get all unique agent names from MongoDB."""
    try:
        mongodb_service = get_mongodb_service()
        agent_names = mongodb_service.get_all_agent_names()

        return jsonify({
            "success": True,
            "agents": agent_names,
            "total_agents": len(agent_names)
        })
    except Exception as e:
        logger.error(f"Error getting agents: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/agents/<agent_name>')
def get_agent_by_name(agent_name):
    """Get agent data by name."""
    try:
        mongodb_service = get_mongodb_service()
        agent_data = mongodb_service.get_agent_by_name(agent_name)

        if agent_data:
            return jsonify({
                "success": True,
                "agent": agent_data
            })
        else:
            return jsonify({
                "success": False,
                "message": f"Agent '{agent_name}' not found"
            }), 404

    except Exception as e:
        logger.error(f"Error getting agent by name: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/reports')
def get_recent_reports():
    """Get recent reports from MongoDB."""
    try:
        limit = request.args.get('limit', 10, type=int)
        mongodb_service = get_mongodb_service()
        reports = mongodb_service.get_recent_reports(limit)

        return jsonify({
            "success": True,
            "reports": reports,
            "total_reports": len(reports)
        })
    except Exception as e:
        logger.error(f"Error getting recent reports: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/reports/<task_id>')
def get_report_by_task_id(task_id):
    """Get report by task ID from MongoDB."""
    try:
        mongodb_service = get_mongodb_service()
        report = mongodb_service.get_report_by_task_id(task_id)

        if report:
            return jsonify({
                "success": True,
                "report": report
            })
        else:
            return jsonify({
                "success": False,
                "message": f"Report with task_id '{task_id}' not found"
            }), 404

    except Exception as e:
        logger.error(f"Error getting report by task_id: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/agents/performance')
def get_agents_by_performance():
    """Get agents ordered by performance metrics."""
    try:
        mongodb_service = get_mongodb_service()
        agents = mongodb_service.get_agents_by_performance()

        return jsonify({
            "success": True,
            "agents": agents,
            "total_agents": len(agents)
        })
    except Exception as e:
        logger.error(f"Error getting agents by performance: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/months')
def get_available_months():
    """Get all available months that have reports."""
    try:
        mongodb_service = get_mongodb_service()
        months = mongodb_service.get_available_months()

        return jsonify({
            "success": True,
            "months": months,
            "total_months": len(months)
        })
    except Exception as e:
        logger.error(f"Error getting available months: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/reports/<int:year>/<int:month>')
def get_reports_by_month(year, month):
    """Get all reports for a specific month."""
    try:
        mongodb_service = get_mongodb_service()
        reports = mongodb_service.get_reports_by_month(year, month)

        return jsonify({
            "success": True,
            "year": year,
            "month": month,
            "reports": reports,
            "total_reports": len(reports)
        })
    except Exception as e:
        logger.error(f"Error getting reports for {year}-{month}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/agents/<int:year>/<int:month>')
def get_agents_by_month(year, month):
    """Get all agents for a specific month."""
    try:
        mongodb_service = get_mongodb_service()
        agents = mongodb_service.get_agents_by_month(year, month)

        return jsonify({
            "success": True,
            "year": year,
            "month": month,
            "agents": agents,
            "total_agents": len(agents)
        })
    except Exception as e:
        logger.error(f"Error getting agents for {year}-{month}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/statistics/<int:year>/<int:month>')
def get_monthly_statistics(year, month):
    """Get statistics for a specific month."""
    try:
        mongodb_service = get_mongodb_service()
        stats = mongodb_service.get_monthly_statistics(year, month)

        return jsonify({
            "success": True,
            "statistics": stats
        })
    except Exception as e:
        logger.error(f"Error getting monthly statistics for {year}-{month}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/agents/<agent_name>/<int:year>/<int:month>')
def get_agent_by_name_and_month(agent_name, year, month):
    """Get agent data by name for a specific month."""
    try:
        mongodb_service = get_mongodb_service()
        agent_data = mongodb_service.get_agent_by_name_and_month(agent_name, year, month)

        if agent_data:
            return jsonify({
                "success": True,
                "year": year,
                "month": month,
                "agent_name": agent_name,
                "agent": agent_data
            })
        else:
            return jsonify({
                "success": False,
                "message": f"Agent '{agent_name}' not found for {year}-{month:02d}"
            }), 404

    except Exception as e:
        logger.error(f"Error getting agent {agent_name} for {year}-{month}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/statistics')
def get_statistics():
    """Get general statistics from MongoDB."""
    try:
        mongodb_service = get_mongodb_service()
        stats = mongodb_service.get_statistics()

        return jsonify({
            "success": True,
            "statistics": stats
        })
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/mongodb/cleanup-bonus', methods=['POST'])
def cleanup_bonus_fields():
    """Remove all bonus-related fields from the database."""
    try:
        mongodb_service = get_mongodb_service()
        result = mongodb_service.cleanup_bonus_fields()

        return jsonify({
            "success": True,
            "message": "Bonus fields cleanup completed",
            "result": result
        })
    except Exception as e:
        logger.error(f"Error cleaning up bonus fields: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/scrape/historical', methods=['POST', 'GET'])
def scrape_historical():
    """
    Start scraping tasks for multiple months going back from current date.

    Parameters:
        months_back: Number of months to go back (default: 12, max: 24)

    Returns:
        List of task IDs for each month being scraped
    """
    try:
        # Get parameters
        data = {}
        try:
            if request.is_json:
                data = request.get_json() or {}
            elif request.form:
                data = request.form.to_dict()
        except Exception:
            data = {}

        if request.method == 'GET':
            data.update(request.args.to_dict())

        months_back = int(data.get('months_back', 12))
        months_back = min(months_back, 24)  # Cap at 24 months

        # Get credentials
        username = data.get('username') or os.environ.get('SCRAPER_USERNAME')
        password = data.get('password') or os.environ.get('SCRAPER_PASSWORD')

        if data.get('username') and data.get('password'):
            os.environ['SCRAPER_USERNAME'] = data.get('username')
            os.environ['SCRAPER_PASSWORD'] = data.get('password')

        # Calculate months to scrape
        current_date = datetime.now()
        months_to_scrape = []

        for i in range(months_back):
            # Calculate the target month
            target_date = current_date - pd.DateOffset(months=i)
            months_to_scrape.append({
                'year': target_date.year,
                'month': target_date.month
            })

        tasks = []

        for month_info in months_to_scrape:
            year = month_info['year']
            month = month_info['month']
            task_id = f"task_{year}{month:02d}_{datetime.now().strftime('%H%M%S')}"

            # Initialize scraper result
            scraper_results[task_id] = {
                "status": "queued",
                "created_at": datetime.now().isoformat(),
                "progress": 0,
                "message": f"Queued for {year}-{month:02d}",
                "task_id": task_id,
                "target_period": f"{year}-{month:02d}"
            }

            tasks.append({
                "task_id": task_id,
                "year": year,
                "month": month,
                "status_url": f"/api/results/{task_id}"
            })

        # Start a background thread that processes each month sequentially
        def run_historical_scraping(tasks_list):
            for task_info in tasks_list:
                task_id = task_info['task_id']
                year = task_info['year']
                month = task_info['month']

                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    scraper_results[task_id]["status"] = "running"
                    scraper_results[task_id]["progress"] = 10
                    scraper_results[task_id]["message"] = f"Scraping {year}-{month:02d}..."

                    scraper = AgentReportScraper(target_year=year, target_month=month)
                    loop.run_until_complete(scraper.scrape())

                    if scraper.scraped_data:
                        scraper_results[task_id]["status"] = "completed"
                        scraper_results[task_id]["progress"] = 100
                        scraper_results[task_id]["data_count"] = len(scraper.scraped_data)
                        scraper_results[task_id]["scraped_data"] = scraper.scraped_data

                        try:
                            mongodb_service = get_mongodb_service()
                            document_id = mongodb_service.save_report(
                                scraper.scraped_data, task_id,
                                target_year=year, target_month=month
                            )
                            scraper_results[task_id]["mongodb_id"] = document_id
                            scraper_results[task_id]["message"] = f"Completed {year}-{month:02d} | MongoDB: {document_id}"
                        except Exception as mongo_error:
                            scraper_results[task_id]["mongodb_error"] = str(mongo_error)
                            scraper_results[task_id]["message"] = f"Completed {year}-{month:02d} (MongoDB error)"
                    else:
                        scraper_results[task_id]["status"] = "completed"
                        scraper_results[task_id]["progress"] = 100
                        scraper_results[task_id]["message"] = f"No data found for {year}-{month:02d}"
                        scraper_results[task_id]["data_count"] = 0

                    loop.close()

                except Exception as e:
                    scraper_results[task_id]["status"] = "error"
                    scraper_results[task_id]["message"] = f"Error scraping {year}-{month:02d}: {str(e)}"
                    logger.error(f"Historical scraping error for {year}-{month}: {e}")

                # Add delay between months to avoid overwhelming the server
                import time
                time.sleep(5)

        thread = threading.Thread(target=run_historical_scraping, args=(tasks,))
        thread.daemon = True
        thread.start()

        return jsonify({
            "success": True,
            "message": f"Started historical scraping for {months_back} months",
            "months_back": months_back,
            "tasks": tasks
        }), 202

    except Exception as e:
        logger.error(f"Error starting historical scraping: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/debug/task/<task_id>')
def debug_task(task_id):
    """DEBUG: Deep inspection of a specific task_id."""
    try:
        mongodb_service = get_mongodb_service()

        # Count using different methods
        count_direct = mongodb_service.agents_collection.count_documents({"task_id": task_id})

        # Count using find
        agents_find = list(mongodb_service.agents_collection.find({"task_id": task_id}))
        count_find = len(agents_find)

        # Get with different projections
        agents_projected = list(mongodb_service.agents_collection.find(
            {"task_id": task_id},
            {"agent_name": 1, "agent_number": 1, "task_id": 1, "_id": 1}
        ))
        count_projected = len(agents_projected)

        # Check for variations in task_id (spaces, case, etc.)
        similar_tasks = list(mongodb_service.agents_collection.find(
            {"task_id": {"$regex": f".*{task_id[-8:]}.*"}},
            {"task_id": 1, "_id": 0}
        ))
        unique_task_ids = list(set(doc["task_id"] for doc in similar_tasks))

        # Get indexes
        indexes = list(mongodb_service.agents_collection.list_indexes())

        return jsonify({
            "success": True,
            "task_id": task_id,
            "counts": {
                "count_documents": count_direct,
                "find_length": count_find,
                "projected_length": count_projected
            },
            "sample_agents": [{"agent_name": a.get("agent_name"), "agent_number": a.get("agent_number")} for a in agents_projected[:5]],
            "similar_task_ids": unique_task_ids,
            "indexes": [{"name": idx.get("name"), "key": idx.get("key"), "unique": idx.get("unique")} for idx in indexes],
            "query_explanation": "Comparing different query methods to find inconsistencies"
        })

    except Exception as e:
        logger.error(f"Debug error for task {task_id}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/debug/reports/<int:year>/<int:month>')
def debug_monthly_reports(year, month):
    """DEBUG: Show all reports available for a specific month."""
    try:
        mongodb_service = get_mongodb_service()

        # Find all reports for the month/year
        reports = list(mongodb_service.reports_collection.find(
            {"year": year, "month": month},
            {"task_id": 1, "saved_at": 1, "report_count": 1, "_id": 1}
        ).sort("saved_at", -1))

        # Get agent counts for each report
        report_details = []
        for report in reports:
            agent_count = mongodb_service.agents_collection.count_documents(
                {"task_id": report["task_id"]}
            )
            report_details.append({
                "task_id": report["task_id"],
                "saved_at": report["saved_at"].isoformat(),
                "report_count": report.get("report_count", 0),
                "agent_count": agent_count,
                "mongodb_id": str(report["_id"])
            })

        return jsonify({
            "success": True,
            "year": year,
            "month": month,
            "total_reports": len(reports),
            "reports": report_details,
            "latest_selected": report_details[0] if report_details else None
        })

    except Exception as e:
        logger.error(f"Debug error for {year}-{month}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/debug/agents-analysis/<int:year>/<int:month>')
def debug_agents_analysis(year, month):
    """DEBUG: Deep analysis of agent data for a specific month."""
    try:
        mongodb_service = get_mongodb_service()

        # Get all agents for this month/year (regardless of task_id)
        all_agents = list(mongodb_service.agents_collection.find(
            {"year": year, "month": month},
            {"agent_name": 1, "task_id": 1, "last_updated": 1, "_id": 1}
        ).sort([("agent_name", 1), ("last_updated", -1)]))

        # Group agents by name to see duplicates and task_id distribution
        agent_groups = {}
        for agent in all_agents:
            name = agent["agent_name"]
            if name not in agent_groups:
                agent_groups[name] = []
            agent_groups[name].append({
                "task_id": agent["task_id"],
                "last_updated": agent["last_updated"].isoformat(),
                "mongodb_id": str(agent["_id"])
            })

        # Get unique task_ids for this month
        unique_task_ids = list(mongodb_service.agents_collection.distinct(
            "task_id",
            {"year": year, "month": month}
        ))

        # Find latest task_id as current logic would select
        latest_report = mongodb_service.reports_collection.find_one(
            {"year": year, "month": month},
            sort=[("saved_at", -1)]
        )

        latest_task_id = latest_report["task_id"] if latest_report else None
        agents_in_latest = mongodb_service.agents_collection.count_documents(
            {"task_id": latest_task_id}
        ) if latest_task_id else 0

        return jsonify({
            "success": True,
            "year": year,
            "month": month,
            "analysis": {
                "total_agent_records": len(all_agents),
                "unique_agent_names": len(agent_groups),
                "unique_task_ids": unique_task_ids,
                "latest_task_id_selected": latest_task_id,
                "agents_in_latest_task": agents_in_latest
            },
            "agent_distribution": agent_groups,
            "summary": {
                "agents_with_multiple_records": len([name for name, records in agent_groups.items() if len(records) > 1]),
                "agents_with_single_record": len([name for name, records in agent_groups.items() if len(records) == 1])
            }
        })

    except Exception as e:
        logger.error(f"Debug agents analysis error for {year}-{month}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/debug/repair-task/<task_id>', methods=['POST'])
def repair_task(task_id):
    """DEBUG: Re-process a report to fix missing agents."""
    try:
        mongodb_service = get_mongodb_service()

        # Find the report document
        report_doc = mongodb_service.reports_collection.find_one({"task_id": task_id})

        if not report_doc:
            return jsonify({
                "success": False,
                "error": f"Report with task_id {task_id} not found"
            }), 404

        # Count agents before repair
        agents_before = mongodb_service.agents_collection.count_documents({"task_id": task_id})

        # Re-process the report data
        agents_processed = 0
        for report in report_doc.get("reports", []):
            agents_processed += mongodb_service._save_agents_from_report(report, task_id)

        # Count agents after repair
        agents_after = mongodb_service.agents_collection.count_documents({"task_id": task_id})

        return jsonify({
            "success": True,
            "task_id": task_id,
            "repair_summary": {
                "agents_before": agents_before,
                "agents_after": agents_after,
                "agents_processed": agents_processed,
                "agents_added": agents_after - agents_before
            },
            "message": f"Successfully repaired task {task_id}"
        })

    except Exception as e:
        logger.error(f"Repair error for task {task_id}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/debug/inspect-agent-schema')
def inspect_agent_schema():
    """DEBUG: Inspect current agent record structure."""
    try:
        mongodb_service = get_mongodb_service()

        # Get a few sample agent records
        sample_agents = list(mongodb_service.agents_collection.find().limit(5))

        # Analyze field presence
        field_analysis = {
            "total_agents": mongodb_service.agents_collection.count_documents({}),
            "has_year_field": 0,
            "has_month_field": 0,
            "has_month_year_field": 0,
            "has_period_field": 0,
            "has_report_timestamp": 0,
            "has_last_updated": 0
        }

        # Check all records for field presence
        all_agents = mongodb_service.agents_collection.find({}, {
            "year": 1, "month": 1, "month_year": 1,
            "period": 1, "report_timestamp": 1, "last_updated": 1
        })

        for agent in all_agents:
            if 'year' in agent:
                field_analysis["has_year_field"] += 1
            if 'month' in agent:
                field_analysis["has_month_field"] += 1
            if 'month_year' in agent:
                field_analysis["has_month_year_field"] += 1
            if 'period' in agent:
                field_analysis["has_period_field"] += 1
            if 'report_timestamp' in agent:
                field_analysis["has_report_timestamp"] += 1
            if 'last_updated' in agent:
                field_analysis["has_last_updated"] += 1

        # Serialize sample agents
        sample_agents_serialized = []
        for agent in sample_agents:
            agent_data = {}
            for key, value in agent.items():
                if key == '_id':
                    agent_data[key] = str(value)
                elif hasattr(value, 'isoformat'):  # datetime
                    agent_data[key] = value.isoformat()
                else:
                    agent_data[key] = value
            sample_agents_serialized.append(agent_data)

        return jsonify({
            "success": True,
            "field_analysis": field_analysis,
            "sample_records": sample_agents_serialized,
            "recommendations": {
                "ready_for_migration": field_analysis["has_year_field"] > 0,
                "needs_date_extraction": field_analysis["has_year_field"] < field_analysis["total_agents"],
                "can_extract_from_month_year": field_analysis["has_month_year_field"] > 0,
                "can_extract_from_period": field_analysis["has_period_field"] > 0,
                "can_extract_from_timestamps": field_analysis["has_report_timestamp"] > 0 or field_analysis["has_last_updated"] > 0
            }
        })

    except Exception as e:
        logger.error(f"Inspect error: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/debug/migrate-to-monthly-schema', methods=['POST'])
def migrate_to_monthly_schema():
    """DEBUG: Migrate existing data to new monthly schema (one record per agent per month)."""
    try:
        mongodb_service = get_mongodb_service()

        # Get all existing agent records
        all_agents = list(mongodb_service.agents_collection.find().sort("last_updated", -1))

        migration_summary = {
            "total_records_before": len(all_agents),
            "records_without_year_month": 0,
            "records_with_year_month": 0,
            "unique_agent_month_combinations": 0,
            "records_to_delete": 0,
            "records_to_keep": 0,
            "duplicates_found": 0
        }

        # First pass - identify records missing year/month and try to extract from other fields
        fixed_agents = []
        records_to_delete_missing_data = []

        for agent in all_agents:
            # Check if agent has year and month fields
            if 'year' not in agent or 'month' not in agent:
                migration_summary["records_without_year_month"] += 1

                # Try to extract year/month from other fields
                year = None
                month = None

                # Try from month_year field (format "2025-08")
                if 'month_year' in agent:
                    try:
                        parts = agent['month_year'].split('-')
                        year = int(parts[0])
                        month = int(parts[1])
                    except:
                        pass

                # Try from period field
                if not year and 'period' in agent and isinstance(agent['period'], dict):
                    year = agent['period'].get('year')
                    month = agent['period'].get('month')

                # Try from report_timestamp
                if not year and 'report_timestamp' in agent:
                    try:
                        from datetime import datetime
                        if isinstance(agent['report_timestamp'], str):
                            dt = datetime.fromisoformat(agent['report_timestamp'].replace('Z', '+00:00'))
                            year = dt.year
                            month = dt.month
                    except:
                        pass

                # Try from last_updated
                if not year and 'last_updated' in agent:
                    try:
                        dt = agent['last_updated']
                        year = dt.year
                        month = dt.month
                    except:
                        pass

                if year and month:
                    # Fix the record by adding year/month fields
                    agent['year'] = year
                    agent['month'] = month
                    fixed_agents.append(agent)
                    logger.info(f"Fixed agent {agent['agent_name']} - extracted date: {year}-{month:02d}")
                else:
                    # Cannot determine year/month - mark for deletion
                    records_to_delete_missing_data.append(agent['_id'])
                    logger.warning(f"Cannot determine date for agent {agent.get('agent_name', 'UNKNOWN')} - marking for deletion")
            else:
                migration_summary["records_with_year_month"] += 1
                fixed_agents.append(agent)

        # Update records that we fixed
        for agent in fixed_agents:
            if agent['_id'] not in [a['_id'] for a in all_agents if 'year' in a and 'month' in a]:
                # This was a fixed record - update it in database
                mongodb_service.agents_collection.update_one(
                    {"_id": agent['_id']},
                    {"$set": {"year": agent['year'], "month": agent['month']}}
                )

        # Delete records we couldn't fix
        if records_to_delete_missing_data:
            delete_result = mongodb_service.agents_collection.delete_many({
                "_id": {"$in": records_to_delete_missing_data}
            })
            logger.info(f"Deleted {delete_result.deleted_count} records with missing date information")

        # Now group agents by name, year, month
        agent_groups = {}
        for agent in fixed_agents:
            key = f"{agent['agent_name']}_{agent['year']}_{agent['month']}"
            if key not in agent_groups:
                agent_groups[key] = []
            agent_groups[key].append(agent)

        migration_summary["unique_agent_month_combinations"] = len(agent_groups)

        # Process each group - keep the most recent record, delete others
        records_to_delete = []
        for key, records in agent_groups.items():
            if len(records) > 1:
                # Multiple records for same agent in same month - keep the most recent
                migration_summary["duplicates_found"] += 1
                # Sort by last_updated descending to get most recent first
                records.sort(key=lambda x: x['last_updated'], reverse=True)

                # Keep the first (most recent) record
                keep_record = records[0]
                migration_summary["records_to_keep"] += 1

                # Mark others for deletion
                for record in records[1:]:
                    records_to_delete.append(record['_id'])
                    migration_summary["records_to_delete"] += 1

                logger.info(f"Agent {keep_record['agent_name']} {keep_record['year']}-{keep_record['month']:02d}: keeping most recent, deleting {len(records)-1} duplicates")
            else:
                # Only one record - keep it
                migration_summary["records_to_keep"] += 1

        # Delete duplicate records
        if records_to_delete:
            delete_result = mongodb_service.agents_collection.delete_many({
                "_id": {"$in": records_to_delete}
            })
            logger.info(f"Deleted {delete_result.deleted_count} duplicate agent records")
            migration_summary["records_deleted"] = delete_result.deleted_count
        else:
            migration_summary["records_deleted"] = 0

        migration_summary["records_deleted_missing_data"] = len(records_to_delete_missing_data)
        migration_summary["total_records_after"] = migration_summary["total_records_before"] - migration_summary["records_deleted"] - migration_summary["records_deleted_missing_data"]

        return jsonify({
            "success": True,
            "message": "Migration to monthly schema completed",
            "migration_summary": migration_summary
        })

    except Exception as e:
        logger.error(f"Migration error: {e}")
        import traceback
        logger.error(f"Migration traceback: {traceback.format_exc()}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/debug/repair-month/<int:year>/<int:month>', methods=['POST'])
def repair_month(year, month):
    """DEBUG: Comprehensive repair for all reports in a specific month."""
    try:
        mongodb_service = get_mongodb_service()

        # Find all reports for this month/year
        reports = list(mongodb_service.reports_collection.find(
            {"year": year, "month": month}
        ).sort("saved_at", 1))  # Process in chronological order

        if not reports:
            return jsonify({
                "success": False,
                "error": f"No reports found for {year}-{month:02d}"
            }), 404

        repair_summary = {
            "total_reports": len(reports),
            "reports_processed": 0,
            "total_agents_before": 0,
            "total_agents_after": 0,
            "agents_added": 0,
            "report_details": []
        }

        # Get total agents before repair
        repair_summary["total_agents_before"] = mongodb_service.agents_collection.count_documents({
            "year": year,
            "month": month
        })

        # Process each report
        for report_doc in reports:
            task_id = report_doc["task_id"]

            # Count agents for this task before repair
            agents_before = mongodb_service.agents_collection.count_documents({"task_id": task_id})

            # Re-process the report data
            agents_processed = 0
            for report in report_doc.get("reports", []):
                if "all_agents" in report:
                    agents_processed += mongodb_service._save_agents_from_report(report, task_id)
                elif "agent_data" in report and "rows" in report["agent_data"]:
                    agents_processed += mongodb_service._save_enhanced_agents_from_report(report, task_id)

            # Count agents for this task after repair
            agents_after = mongodb_service.agents_collection.count_documents({"task_id": task_id})

            report_detail = {
                "task_id": task_id,
                "saved_at": report_doc["saved_at"].isoformat(),
                "agents_before": agents_before,
                "agents_after": agents_after,
                "agents_processed": agents_processed,
                "agents_added": agents_after - agents_before
            }

            repair_summary["report_details"].append(report_detail)
            repair_summary["reports_processed"] += 1

            logger.info(f"Repaired task {task_id}: {agents_before} -> {agents_after} agents")

        # Get total agents after repair
        repair_summary["total_agents_after"] = mongodb_service.agents_collection.count_documents({
            "year": year,
            "month": month
        })

        repair_summary["agents_added"] = repair_summary["total_agents_after"] - repair_summary["total_agents_before"]

        return jsonify({
            "success": True,
            "year": year,
            "month": month,
            "repair_summary": repair_summary,
            "message": f"Successfully repaired all reports for {year}-{month:02d}"
        })

    except Exception as e:
        logger.error(f"Month repair error for {year}-{month}: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    debug = os.environ.get('FLASK_ENV') == 'development'

    logger.info(f"Starting Agent Report Scraper API on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
