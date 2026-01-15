import logging
import logging.handlers
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict
from app.config import settings

class StructuredFormatter(logging.Formatter):
    """JSON structured logging formatter"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "node_id": settings.NODE_ID,
            "node_type": settings.NODE_TYPE,
            "message": record.getMessage(),
        }
        
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        
        if hasattr(record, "extra"):
            log_obj.update(record.extra)
        
        return json.dumps(log_obj)

class TelecomLogger:
    """Telecom-specific logger with performance metrics"""
    
    @staticmethod
    def setup_telecom_logger(name: str) -> logging.Logger:
        """Setup telecom-optimized logger"""
        logger = logging.getLogger(name)
        
        if logger.handlers:
            return logger
        
        logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
        logger.propagate = False
        
        # Console handler (structured JSON for production, readable for dev)
        console = logging.StreamHandler(sys.stdout)
        if settings.LOG_LEVEL.upper() == "DEBUG":
            console.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                datefmt="%H:%M:%S"
            ))
        else:
            console.setFormatter(StructuredFormatter())
        logger.addHandler(console)
        
        # File handler (rotating, structured JSON)
        log_dir = Path(settings.LOG_FILE).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            settings.LOG_FILE,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=10,
        )
        file_handler.setFormatter(StructuredFormatter())
        logger.addHandler(file_handler)
        
        # Metrics file handler (CSV for analysis)
        metrics_dir = Path(settings.METRICS_FILE).parent
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        metrics_handler = logging.handlers.RotatingFileHandler(
            settings.METRICS_FILE,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=5,
        )
        metrics_handler.setLevel(logging.INFO)
        metrics_handler.addFilter(lambda record: hasattr(record, 'metric'))
        metrics_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(metrics_handler)
        
        return logger
    
    @staticmethod
    def log_metric(logger: logging.Logger, metric_type: str, value: float, tags: Dict[str, Any] = None):
        """Log a performance metric"""
        metric_record = logger.makeRecord(
            name=logger.name,
            level=logging.INFO,
            fn="",
            lno=0,
            msg="",
            args=(),
            exc_info=None
        )
        
        metric_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "node_id": settings.NODE_ID,
            "metric_type": metric_type,
            "value": value,
            **(tags or {})
        }
        
        # Log to metrics file (CSV-like)
        logger.info(
            f"{metric_data['timestamp']},{settings.NODE_ID},{metric_type},{value}"
            + (f",{json.dumps(tags)}" if tags else "")
        )
        
        # Also log to regular log with metric tag
        metric_record.extra = {"metric": metric_data}
        logger.handle(metric_record)

# Convenience function
def setup_logger(name: str) -> logging.Logger:
    return TelecomLogger.setup_telecom_logger(name)