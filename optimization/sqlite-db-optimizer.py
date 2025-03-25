import sqlite3
from typing import List, Dict, Tuple
import logging
from collections import defaultdict

class DatabaseOptimizer:
    def __init__(self, db_path: str):
        """Initialize the database optimizer with the path to SQLite database."""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            self.logger.info(f"Successfully connected to database: {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")

    def analyze_table_structure(self) -> Dict:
        """Analyze table structures and relationships."""
        analysis = defaultdict(dict)
        
        # Get list of all tables
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = self.cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            # Get table info
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            
            # Get index information
            self.cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = self.cursor.fetchall()
            
            # Get foreign key information
            self.cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = self.cursor.fetchall()
            
            analysis[table_name] = {
                'columns': columns,
                'indexes': indexes,
                'foreign_keys': foreign_keys
            }
            
        return analysis

    def check_normalization(self) -> List[Dict]:
        """Check for potential normalization issues."""
        issues = []
        
        # Get all tables
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = self.cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            
            # Check for multiple values in single columns (1NF violation)
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            
            for column in columns:
                column_name = column[1]
                # Sample data to check for delimiter characters
                self.cursor.execute(f"SELECT {column_name} FROM {table_name} LIMIT 100")
                samples = self.cursor.fetchall()
                
                for sample in samples:
                    if sample[0] and isinstance(sample[0], str):
                        delimiters = [',', ';', '|', '/']
                        if any(d in sample[0] for d in delimiters):
                            issues.append({
                                'table': table_name,
                                'column': column_name,
                                'issue': 'Possible 1NF violation - multiple values in single column',
                                'sample': sample[0]
                            })
        
        return issues

    def suggest_indexes(self) -> List[Dict]:
        """Suggest indexes based on query patterns and table structure."""
        suggestions = []
        
        # Get all tables
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = self.cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            
            # Get existing indexes
            self.cursor.execute(f"PRAGMA index_list({table_name})")
            existing_indexes = set(idx[1] for idx in self.cursor.fetchall())
            
            # Get column info
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            
            # Suggest indexes for foreign key columns if not already indexed
            self.cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = self.cursor.fetchall()
            
            for fk in foreign_keys:
                idx_name = f"idx_{table_name}_{fk[3]}"
                if idx_name not in existing_indexes:
                    suggestions.append({
                        'table': table_name,
                        'column': fk[3],
                        'suggestion': f'Create index on foreign key column: {fk[3]}',
                        'sql': f'CREATE INDEX {idx_name} ON {table_name}({fk[3]})'
                    })
        
        return suggestions

    def optimize_queries(self) -> List[str]:
        """Generate optimized queries for common operations."""
        optimized_queries = []
        
        # Add basic query optimization templates
        optimized_queries.extend([
            "-- Use EXISTS instead of IN for better performance",
            "SELECT * FROM table1 WHERE EXISTS (SELECT 1 FROM table2 WHERE table2.id = table1.id)",
            
            "-- Use JOIN instead of subqueries where possible",
            "SELECT t1.*, t2.* FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id",
            
            "-- Use EXPLAIN QUERY PLAN to analyze query performance",
            "EXPLAIN QUERY PLAN SELECT * FROM your_table WHERE your_column = 'value'"
        ])
        
        return optimized_queries

    def generate_optimization_report(self) -> Dict:
        """Generate a comprehensive optimization report."""
        try:
            self.connect()
            
            report = {
                'table_analysis': self.analyze_table_structure(),
                'normalization_issues': self.check_normalization(),
                'index_suggestions': self.suggest_indexes(),
                'query_optimizations': self.optimize_queries()
            }
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating optimization report: {e}")
            raise
        finally:
            self.close()

    def execute_optimization(self, optimize_indexes: bool = True, vacuum: bool = True):
        """Execute optimization recommendations."""
        try:
            self.connect()
            
            if optimize_indexes:
                suggestions = self.suggest_indexes()
                for suggestion in suggestions:
                    try:
                        self.cursor.execute(suggestion['sql'])
                        self.logger.info(f"Created index: {suggestion['sql']}")
                    except sqlite3.Error as e:
                        self.logger.error(f"Error creating index: {e}")
            
            if vacuum:
                self.cursor.execute("VACUUM")
                self.logger.info("Database vacuumed successfully")
            
            self.conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error during optimization execution: {e}")
            raise
        finally:
            self.close()
