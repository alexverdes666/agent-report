"""
MongoDB service for storing and retrieving agent reports.
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
from bson import ObjectId
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from config import MONGODB_CONFIG

logger = logging.getLogger(__name__)


class MongoDBService:
    """Service class for MongoDB operations."""

    def __init__(self):
        """Initialize MongoDB connection."""
        self.client = None
        self.db = None
        self.reports_collection = None
        self.agents_collection = None
        self._connect()

    def _connect(self):
        """Establish connection to MongoDB."""
        try:
            self.client = MongoClient(
                MONGODB_CONFIG["connection_string"],
                serverSelectionTimeoutMS=MONGODB_CONFIG["connection_timeout"],
                maxPoolSize=MONGODB_CONFIG["max_pool_size"]
            )

            # Test connection
            self.client.admin.command('ping')

            # Get database and collections
            self.db = self.client[MONGODB_CONFIG["database_name"]]
            self.reports_collection = self.db[MONGODB_CONFIG["collection_name"]]
            self.agents_collection = self.db[MONGODB_CONFIG["agents_collection"]]

            # Create indexes for better performance
            self._create_indexes()

            logger.info("Connected to MongoDB successfully")

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def _create_indexes(self):
        """Create database indexes for better query performance."""
        try:
            # First, clean up the problematic conflicting index
            self._cleanup_conflicting_indexes()

            # Indexes for reports collection
            self.reports_collection.create_index([("timestamp", DESCENDING)])
            self.reports_collection.create_index([("task_id", ASCENDING)])
            self.reports_collection.create_index([("all_agents.agent_name", ASCENDING)])
            self.reports_collection.create_index([("all_agents.agent_number", ASCENDING)])

            # Monthly organization indexes
            self.reports_collection.create_index([("year", DESCENDING), ("month", DESCENDING)])
            self.reports_collection.create_index([("month_year", DESCENDING)])
            self.reports_collection.create_index([("saved_at", DESCENDING)])

            # Compound index for efficient agent queries
            self.reports_collection.create_index([
                ("timestamp", DESCENDING),
                ("all_agents.agent_name", ASCENDING)
            ])

            # Indexes for agents collection
            # Compound unique index: one record per agent per month
            self.agents_collection.create_index([
                ("agent_name", ASCENDING),
                ("year", ASCENDING),
                ("month", ASCENDING)
            ], unique=True)
            self.agents_collection.create_index([("agent_number", ASCENDING)])
            self.agents_collection.create_index([("last_updated", DESCENDING)])

            # Monthly organization indexes for agents
            self.agents_collection.create_index([("year", DESCENDING), ("month", DESCENDING)])
            self.agents_collection.create_index([("month_year", DESCENDING)])

            # Compound indexes for efficient monthly queries
            self.agents_collection.create_index([
                ("year", DESCENDING),
                ("month", DESCENDING),
                ("agent_name", ASCENDING)
            ])

            logger.info("Database indexes created successfully")

        except Exception as e:
            logger.error(f"Error creating indexes: {e}")

    def _cleanup_conflicting_indexes(self):
        """Remove conflicting indexes from previous schema designs."""
        try:
            # List current indexes to see what exists
            existing_indexes = list(self.agents_collection.list_indexes())

            indexes_to_drop = []

            # Check for old conflicting indexes
            for idx in existing_indexes:
                idx_name = idx.get('name')
                idx_key = idx.get('key', {})

                # Drop old standalone agent_name unique index
                if (idx_name == 'agent_name_1' and
                    idx_key == {'agent_name': 1} and
                    idx.get('unique') == True):
                    indexes_to_drop.append('agent_name_1')

                # Drop old agent_name + task_id compound index
                elif (idx_name == 'agent_name_1_task_id_1' and
                      idx_key == {'agent_name': 1, 'task_id': 1}):
                    indexes_to_drop.append('agent_name_1_task_id_1')

            # Drop conflicting indexes
            for idx_name in indexes_to_drop:
                logger.info(f"Found conflicting index {idx_name}, dropping it...")
                self.agents_collection.drop_index(idx_name)
                logger.info(f"Dropped conflicting index: {idx_name}")

        except Exception as e:
            # If cleanup fails, that's okay - proceed with index creation
            logger.info(f"Index cleanup info: {e}")

    def _extract_report_date(self, report: Dict, task_id: str) -> datetime:
        """
        Extract the proper report date from report data for monthly organization.

        Args:
            report: Report dictionary
            task_id: Task identifier for logging

        Returns:
            datetime: The date this report represents
        """
        # Try to extract date from report timestamp
        report_timestamp = report.get("timestamp")
        if report_timestamp:
            try:
                if isinstance(report_timestamp, str):
                    # Handle ISO format timestamps
                    report_date = datetime.fromisoformat(report_timestamp.replace('Z', '+00:00'))
                    logger.debug(f"Task {task_id}: Using report timestamp {report_date}")
                    return report_date.replace(tzinfo=None)  # Store as UTC naive datetime
                elif isinstance(report_timestamp, datetime):
                    logger.debug(f"Task {task_id}: Using report timestamp {report_timestamp}")
                    return report_timestamp
            except Exception as e:
                logger.warning(f"Task {task_id}: Could not parse report timestamp '{report_timestamp}': {e}")

        # Fallback: Extract from task_id if it contains a date (format: task_YYYYMMDD_HHMMSS)
        try:
            if task_id.startswith('task_') and len(task_id) >= 13:
                date_part = task_id[5:13]  # YYYYMMDD
                task_date = datetime.strptime(date_part, '%Y%m%d')
                logger.info(f"Task {task_id}: Extracted date from task_id: {task_date}")
                return task_date
        except Exception as e:
            logger.warning(f"Task {task_id}: Could not extract date from task_id: {e}")

        # Final fallback: use current time but warn
        current_time = datetime.utcnow()
        logger.warning(f"Task {task_id}: No report date found, using current time: {current_time}")
        return current_time

    def _serialize_mongodb_doc(self, doc):
        """Convert MongoDB document to JSON-serializable format and filter out bonus fields."""
        if isinstance(doc, dict):
            # Filter out bonus-related fields
            filtered_doc = {
                key: (str(value) if isinstance(value, ObjectId)
                      else self._serialize_mongodb_doc(value))
                for key, value in doc.items()
                if not key.startswith('bonus') and key != 'bonus_calculation'
            }
            return filtered_doc
        elif isinstance(doc, list):
            return [self._serialize_mongodb_doc(item) for item in doc]
        elif isinstance(doc, ObjectId):
            return str(doc)
        elif isinstance(doc, datetime):
            return doc.isoformat()
        return doc

    def save_report(self, report_data: List[Dict], task_id: str, target_year: int = None, target_month: int = None) -> str:
        """
        Save a complete report to MongoDB with monthly organization.

        Args:
            report_data: List of report data (as loaded from JSON)
            task_id: Unique task identifier
            target_year: Optional target year for historical data (defaults to current year)
            target_month: Optional target month for historical data (defaults to current month)

        Returns:
            str: Document ID of the saved report
        """
        try:
            current_time = datetime.utcnow()

            # Use target year/month if provided, otherwise use current date
            report_year = target_year if target_year else current_time.year
            report_month = target_month if target_month else current_time.month

            # Create a date object for the report period
            report_date = datetime(report_year, report_month, 1)

            # Prepare document with monthly fields
            document = {
                "task_id": task_id,
                "saved_at": current_time,
                "report_count": len(report_data),
                "reports": report_data,
                # Monthly organization fields - use target date
                "year": report_year,
                "month": report_month,
                "month_year": f"{report_year}-{report_month:02d}",
                "period": {
                    "year": report_year,
                    "month": report_month,
                    "month_name": report_date.strftime("%B"),
                    "month_year_display": report_date.strftime("%B %Y")
                }
            }

            # Extract and save individual agent data
            agents_saved = 0
            for report in report_data:
                # Handle both basic and enhanced scraping data structures
                if "all_agents" in report:
                    # Basic scraping structure
                    agents_saved += self._save_agents_from_report(report, task_id)
                elif "agent_data" in report and "rows" in report["agent_data"]:
                    # Enhanced scraping structure
                    agents_saved += self._save_enhanced_agents_from_report(report, task_id)

            # Save the complete report
            result = self.reports_collection.insert_one(document)

            logger.info(f"Saved report {task_id} with {len(report_data)} records and {agents_saved} agents")
            return str(result.inserted_id)

        except Exception as e:
            logger.error(f"Error saving report: {e}")
            raise

    def _save_agents_from_report(self, report: Dict, task_id: str) -> int:
        """
        Extract and save individual agent records from a report with monthly organization.

        Args:
            report: Single report dictionary
            task_id: Task identifier

        Returns:
            int: Number of agents processed
        """
        agents_processed = 0

        try:
            current_time = datetime.utcnow()

            # Extract the report date for proper monthly organization
            report_date = self._extract_report_date(report, task_id)

            for agent in report.get("all_agents", []):
                agent_doc = {
                    "agent_name": agent.get("agent_name"),
                    "agent_number": agent.get("agent_number"),
                    "task_id": task_id,
                    "report_timestamp": report.get("timestamp"),
                    "last_updated": current_time,
                    "incoming_calls": agent.get("incoming_calls", {}),
                    "outgoing_calls": agent.get("outgoing_calls", {}),
                    "actions": agent.get("actions", ""),
                    "row_index": agent.get("row_index"),
                    # Monthly organization fields - use report date, not current time
                    "year": report_date.year,
                    "month": report_date.month,
                    "month_year": f"{report_date.year}-{report_date.month:02d}",
                    "period": {
                        "year": report_date.year,
                        "month": report_date.month,
                        "month_name": report_date.strftime("%B"),
                        "month_year_display": report_date.strftime("%B %Y")
                    }
                }

                # Update or insert agent data (one record per agent per month)
                self.agents_collection.update_one(
                    {
                        "agent_name": agent.get("agent_name"),
                        "year": report_date.year,
                        "month": report_date.month
                    },
                    {"$set": agent_doc},
                    upsert=True
                )
                agents_processed += 1

        except Exception as e:
            logger.error(f"Error saving agents from report: {e}")

        return agents_processed

    def _save_enhanced_agents_from_report(self, report: Dict, task_id: str) -> int:
        """
        Extract and save individual agent records from an enhanced report with monthly organization.
        Handles enhanced data with call_details.

        Args:
            report: Enhanced report dictionary
            task_id: Task identifier

        Returns:
            int: Number of agents processed
        """
        agents_processed = 0

        try:
            current_time = datetime.utcnow()

            # Extract the report date for proper monthly organization
            report_date = self._extract_report_date(report, task_id)

            agents = report.get("agent_data", {}).get("rows", [])

            for agent in agents:
                agent_doc = {
                    "agent_name": agent.get("agent_name"),
                    "agent_number": agent.get("agent_number"),
                    "task_id": task_id,
                    "report_timestamp": report.get("timestamp"),
                    "last_updated": current_time,
                    "incoming_calls": agent.get("incoming_calls", {}),
                    "outgoing_calls": agent.get("outgoing_calls", {}),
                    "actions": agent.get("actions", ""),
                    "row_index": agent.get("row_index"),
                    # Enhanced fields
                    "call_details": agent.get("call_details", {}),
                    # Monthly organization fields - use report date, not current time
                    "year": report_date.year,
                    "month": report_date.month,
                    "month_year": f"{report_date.year}-{report_date.month:02d}",
                    "period": {
                        "year": report_date.year,
                        "month": report_date.month,
                        "month_name": report_date.strftime("%B"),
                        "month_year_display": report_date.strftime("%B %Y")
                    }
                }

                # Update or insert agent data (one record per agent per month)
                self.agents_collection.update_one(
                    {
                        "agent_name": agent.get("agent_name"),
                        "year": report_date.year,
                        "month": report_date.month
                    },
                    {"$set": agent_doc},
                    upsert=True
                )
                agents_processed += 1

        except Exception as e:
            logger.error(f"Error saving enhanced agents from report: {e}")

        return agents_processed

    def get_reports_by_task_id(self, task_id: str) -> Optional[Dict]:
        """Get reports by task ID."""
        try:
            report = self.reports_collection.find_one({"task_id": task_id})
            return self._serialize_mongodb_doc(report) if report else None
        except Exception as e:
            logger.error(f"Error getting reports by task ID: {e}")
            return None

    def get_agent_data(self, agent_name: str, limit: int = 100) -> List[Dict]:
        """
        Get agent data by name.

        Args:
            agent_name: Name of the agent
            limit: Maximum number of records to return

        Returns:
            List of agent records
        """
        try:
            cursor = self.agents_collection.find(
                {"agent_name": {"$regex": agent_name, "$options": "i"}},
                sort=[("last_updated", DESCENDING)],
                limit=limit
            )
            return [self._serialize_mongodb_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error(f"Error getting agent data: {e}")
            return []

    def get_agent_by_name(self, agent_name: str) -> Optional[Dict]:
        """
        Get the most recent agent data by name.

        Args:
            agent_name: Name of the agent

        Returns:
            Most recent agent record or None if not found
        """
        try:
            agent = self.agents_collection.find_one(
                {"agent_name": {"$regex": f"^{agent_name}$", "$options": "i"}},
                sort=[("last_updated", DESCENDING)]
            )
            return self._serialize_mongodb_doc(agent) if agent else None
        except Exception as e:
            logger.error(f"Error getting agent by name: {e}")
            return None

    def get_agent_by_name_and_month(self, agent_name: str, year: int, month: int) -> Optional[Dict]:
        """
        Get agent data by name for a specific month.

        Args:
            agent_name: Name of the agent
            year: Year (e.g., 2024)
            month: Month (1-12)

        Returns:
            Agent record for the specified month or None if not found
        """
        try:
            agent = self.agents_collection.find_one({
                "agent_name": {"$regex": f"^{agent_name}$", "$options": "i"},
                "year": year,
                "month": month
            })
            return self._serialize_mongodb_doc(agent) if agent else None
        except Exception as e:
            logger.error(f"Error getting agent {agent_name} for {year}-{month}: {e}")
            return None

    def get_all_agent_names(self) -> List[str]:
        """Get all unique agent names."""
        try:
            return self.agents_collection.distinct("agent_name")
        except Exception as e:
            logger.error(f"Error getting agent names: {e}")
            return []

    def get_recent_reports(self, limit: int = 10) -> List[Dict]:
        """Get recent reports."""
        try:
            cursor = self.reports_collection.find(
                {},
                {"task_id": 1, "saved_at": 1, "report_count": 1},
                sort=[("saved_at", DESCENDING)],
                limit=limit
            )
            return [self._serialize_mongodb_doc(doc) for doc in cursor]
        except Exception as e:
            logger.error(f"Error getting recent reports: {e}")
            return []

    def search_agents_by_performance(self, min_calls: int = 0) -> List[Dict]:
        """
        Search agents by performance criteria.

        Args:
            min_calls: Minimum number of total incoming calls

        Returns:
            List of agent records matching criteria
        """
        try:
            pipeline = [
                {
                    "$addFields": {
                        "total_calls": {
                            "$toInt": "$incoming_calls.total"
                        }
                    }
                },
                {
                    "$match": {
                        "total_calls": {"$gte": min_calls}
                    }
                },
                {
                    "$sort": {"total_calls": -1}
                }
            ]

            results = list(self.agents_collection.aggregate(pipeline))
            return [self._serialize_mongodb_doc(doc) for doc in results]
        except Exception as e:
            logger.error(f"Error searching agents by performance: {e}")
            return []

    def get_available_months(self) -> List[Dict[str, Any]]:
        """Get all available months that have reports."""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": {
                            "year": "$year",
                            "month": "$month",
                            "month_year": "$month_year"
                        },
                        "report_count": {"$sum": 1},
                        "first_report": {"$min": "$saved_at"},
                        "last_report": {"$max": "$saved_at"}
                    }
                },
                {
                    "$project": {
                        "year": "$_id.year",
                        "month": "$_id.month",
                        "month_year": "$_id.month_year",
                        "month_name": {
                            "$arrayElemAt": [
                                ["", "January", "February", "March", "April", "May", "June",
                                 "July", "August", "September", "October", "November", "December"],
                                "$_id.month"
                            ]
                        },
                        "report_count": 1,
                        "first_report": 1,
                        "last_report": 1,
                        "_id": 0
                    }
                },
                {
                    "$sort": {"year": -1, "month": -1}
                }
            ]

            results = list(self.reports_collection.aggregate(pipeline))
            return [self._serialize_mongodb_doc(doc) for doc in results]
        except Exception as e:
            logger.error(f"Error getting available months: {e}")
            return []

    def get_reports_by_month(self, year: int, month: int) -> List[Dict[str, Any]]:
        """Get all reports for a specific month."""
        try:
            query = {"year": year, "month": month}
            reports = list(self.reports_collection.find(query).sort("saved_at", -1))
            return [self._serialize_mongodb_doc(doc) for doc in reports]
        except Exception as e:
            logger.error(f"Error getting reports for {year}-{month}: {e}")
            return []

    def get_agents_by_month(self, year: int, month: int) -> List[Dict[str, Any]]:
        """Get all agents for a specific month from the latest report only."""
        try:
            # First, find the latest report for this month/year
            latest_report = self.reports_collection.find_one(
                {"year": year, "month": month},
                sort=[("saved_at", -1)]
            )
            
            if not latest_report:
                logger.warning(f"No reports found for {year}-{month:02d}")
                return []
            
            latest_task_id = latest_report["task_id"]
            logger.info(f"Using latest task {latest_task_id} for {year}-{month:02d}")
            
            # Get agents from the latest report only
            agents = list(self.agents_collection.find(
                {
                    "year": year, 
                    "month": month,
                    "task_id": latest_task_id
                }
            ).sort("agent_name", 1))

            logger.info(f"Retrieved {len(agents)} agents from latest report for {year}-{month:02d}")
            return [self._serialize_mongodb_doc(doc) for doc in agents]

        except Exception as e:
            logger.error(f"Error getting agents for {year}-{month}: {e}")
            return []

    def get_monthly_statistics(self, year: int, month: int) -> Dict[str, Any]:
        """Get statistics for a specific month."""
        try:
            # With the new schema, we can directly query agents for this month
            agent_query = {"year": year, "month": month}
            total_agents = self.agents_collection.count_documents(agent_query)
            total_reports = self.reports_collection.count_documents({"year": year, "month": month})

            if total_agents == 0:
                logger.warning(f"No agents found for statistics {year}-{month}")
                return {
                    "year": year,
                    "month": month,
                    "month_name": ["", "January", "February", "March", "April", "May", "June",
                                  "July", "August", "September", "October", "November", "December"][month],
                    "total_agents": 0,
                    "total_reports": total_reports,
                    "error": "No agent data found for this month"
                }

            logger.info(f"Getting statistics for {total_agents} agents in {year}-{month}")

            # Get statistics from all agents in this month

            # Call statistics aggregation from latest report only
            pipeline = [
                {"$match": agent_query},
                {
                    "$group": {
                        "_id": None,
                        "total_incoming_calls": {
                            "$sum": {
                                "$cond": [
                                    {"$isNumber": "$incoming_calls.total"},
                                    "$incoming_calls.total",
                                    0
                                ]
                            }
                        },
                        "total_outgoing_calls": {
                            "$sum": {
                                "$cond": [
                                    {"$isNumber": "$outgoing_calls.total"},
                                    "$outgoing_calls.total",
                                    0
                                ]
                            }
                        },
                        "agents_with_calls": {
                            "$sum": {
                                "$cond": [
                                    {
                                        "$or": [
                                            {"$gt": ["$incoming_calls.total", 0]},
                                            {"$gt": ["$outgoing_calls.total", 0]}
                                        ]
                                    },
                                    1,
                                    0
                                ]
                            }
                        }
                    }
                }
            ]

            call_stats = list(self.agents_collection.aggregate(pipeline))

            stats = {
                "year": year,
                "month": month,
                "month_name": ["", "January", "February", "March", "April", "May", "June",
                              "July", "August", "September", "October", "November", "December"][month],
                "total_agents": total_agents,
                "total_reports": total_reports,
                "total_incoming_calls": 0,
                "total_outgoing_calls": 0,
                "total_calls": 0,
                "agents_with_calls": 0,
                "average_calls_per_agent": 0
            }

            if call_stats:
                call_data = call_stats[0]
                stats.update({
                    "total_incoming_calls": call_data.get("total_incoming_calls", 0),
                    "total_outgoing_calls": call_data.get("total_outgoing_calls", 0),
                    "agents_with_calls": call_data.get("agents_with_calls", 0)
                })

                stats["total_calls"] = stats["total_incoming_calls"] + stats["total_outgoing_calls"]

                if stats["agents_with_calls"] > 0:
                    stats["average_calls_per_agent"] = round(stats["total_calls"] / stats["agents_with_calls"], 2)

            return stats

        except Exception as e:
            logger.error(f"Error getting monthly statistics for {year}-{month}: {e}")
            return {
                "year": year,
                "month": month,
                "error": str(e)
            }

    def get_agent_statistics(self) -> Dict[str, Any]:
        """Get overall agent statistics."""
        try:
            stats = {
                "total_agents": self.agents_collection.count_documents({}),
                "total_reports": self.reports_collection.count_documents({}),
                "unique_agent_names": len(self.get_all_agent_names())
            }

            # Get latest report timestamp
            latest_report = self.reports_collection.find_one(
                {},
                sort=[("saved_at", DESCENDING)]
            )

            if latest_report:
                stats["latest_report"] = latest_report["saved_at"].isoformat()

            return stats
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}

    def cleanup_bonus_fields(self) -> Dict[str, int]:
        """Remove bonus-related fields from all existing documents."""
        try:
            # Update agents collection - remove bonus fields
            agents_result = self.agents_collection.update_many(
                {},
                {"$unset": {"bonus_calculation": "", "bonus": ""}}
            )

            # Update reports collection - remove bonus fields from nested agent data
            reports_result = self.reports_collection.update_many(
                {},
                {
                    "$unset": {
                        "reports.$[].all_agents.$[].bonus_calculation": "",
                        "reports.$[].all_agents.$[].bonus": "",
                        "reports.$[].agent_data.rows.$[].bonus_calculation": "",
                        "reports.$[].agent_data.rows.$[].bonus": ""
                    }
                }
            )

            logger.info(f"Cleaned bonus fields from {agents_result.modified_count} agents and {reports_result.modified_count} reports")

            return {
                "agents_updated": agents_result.modified_count,
                "reports_updated": reports_result.modified_count
            }

        except Exception as e:
            logger.error(f"Error cleaning up bonus fields: {e}")
            return {"error": str(e)}


    def close_connection(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


# Global instance
mongodb_service = None

def get_mongodb_service() -> MongoDBService:
    """Get or create MongoDB service instance."""
    global mongodb_service
    if mongodb_service is None:
        mongodb_service = MongoDBService()
    return mongodb_service