# app/semantic/registry.py
from dataclasses import dataclass
from typing import Dict, List
import os
import yaml

# from semantic.model import facts, dimensions, measures, metrics, dates
from semantic.types import (
    Fact,
    Dimension,
    Measure,
    Metric,
    DateDimension,
)
import os
import prestodb
from prestodb.auth import BasicAuthentication
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Semantic Core Objects
# ----------------------------

@dataclass(frozen=True)
class Fact:
    name: str
    table: str
    grain: str
    primary_date: str

@dataclass(frozen=True)
class DateDimension:
    name: str
    column: str
    sql: str
    supported_filters: List[str]
    supported_grains: List[str]

@dataclass(frozen=True)
class Dimension:
    name: str
    column: str
    type: str

@dataclass(frozen=True)
class Measure:
    name: str
    column: str
    aggregation: str
    mandatory_filters: List[str]
    sql: str = None  # NEW: Support for raw SQL expressions (e.g. for cleaning data)

@dataclass(frozen=True)
class Metric:
    name: str
    description: str
    expression: str
    behavior: str
    filters: List[str] = None  # NEW: Support for metric-level filters
    min_safe_grain: str | None = None

# ----------------------------
# Semantic Registry
# ----------------------------

class SemanticRegistry:
    """
    Loads and holds the complete semantic contract.
    NOTHING outside this class should read YAML directly.
    """

    # def __init__(self, model_path: str):
    #     self.model_path = model_path
    #     self.Fact: Fact | None = None
    #     self.DateDimension: Dict[str, DateDimension] = {}
    #     self.Dimension: Dict[str, Dimension] = {}
    #     self.Measure: Dict[str, Measure] = {}
    #     self.Metric: Dict[str, Metric] = {}
    #     self._load_all()
    #     self._basic_validate()
    #     self.dates: Dict[str, DateDimension] = {}
        
    #     # Presto connection setup
    #     self._setup_presto_connection()
    def __init__(self, model_path: str):
        self.model_path = model_path

        # Core objects
        self.fact: Fact | None = None

        # Registries (MUST exist before loading)
        self.dates: Dict[str, DateDimension] = {}
        self.dimensions: Dict[str, Dimension] = {}
        self.measures: Dict[str, Measure] = {}
        self.metrics: Dict[str, Metric] = {}

        # Load everything
        self._load_all()
        self._basic_validate()

        # Presto connection setup
        self._setup_presto_connection()
    # ----------------------------
    # NEW: Presto Connection Setup
    # ----------------------------
    
    def _setup_presto_connection(self):
        """Initialize Presto connection parameters from environment"""
        self.presto_config = {
            'host': os.getenv('PRESTO_HOST'),
            'port': int(os.getenv('PRESTO_PORT', 8080)),
            'user': os.getenv('PRESTO_USER'),
            'catalog': os.getenv('PRESTO_CATALOG'),
            'schema': os.getenv('PRESTO_SCHEMA'),
            'password': os.getenv('PRESTO_PASSWORD')
        }

    def execute(self, sql: str) -> Dict:
        """
        Execute SQL query on Presto and return results.
        
        Args:
            sql: SQL query string
            
        Returns:
            Dictionary with 'columns' and 'rows' keys
        """
        try:
            # ADD THIS:
            print(f"\n{'='*80}\nSQL:\n{'='*80}\n{sql}\n{'='*80}\n")
            conn = prestodb.dbapi.connect(
                host=self.presto_config['host'],
                port=self.presto_config['port'],
                user=self.presto_config['user'],
                catalog=self.presto_config['catalog'],
                schema=self.presto_config['schema'],
                http_scheme='https',
                auth=BasicAuthentication(
                    self.presto_config['user'],
                    self.presto_config['password']
                )
            )
            
            cursor = conn.cursor()
            cursor.execute(sql)
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return {
                'columns': columns,
                'rows': rows
            }
            
        except Exception as e:
            raise RuntimeError(f"Presto execution failed: {str(e)}")

    # ----------------------------
    # YAML Loaders
    # ----------------------------

    def _load_yaml(self, filename: str) -> dict:
        print(f"DEBUG: Attempting to load YAML file: {filename} from {self.model_path}")
        path = self.model_path + "/" + filename

        print(f"DEBUG: Loading YAML file from path: {path}")

        print("DEBUG: Current working directory:", os.getcwd())
        print("DEBUG: Absolute path checked:", os.path.abspath(path))
        print("DEBUG: File exists:", os.path.exists(os.path.abspath(path)))
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing semantic file: {filename}")
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _load_all(self):
        self._load_fact()
        self._load_dates()
        self._load_dimensions()
        self._load_measures()
        self._load_metrics()

    # In registry.py, update the _load_fact method:

    def _load_fact(self):
        data = self._load_yaml("facts.yaml")
        
        # Debug: Print what we actually loaded
        print(f"DEBUG: Loaded facts.yaml keys: {list(data.keys())}")
        print(f"DEBUG: Full data: {data}")
        
        if len(data) != 1:
            raise ValueError(f"facts.yaml must define exactly ONE fact. Found: {list(data.keys())}")
        
        name, cfg = next(iter(data.items()))
        print(f"DEBUG: Fact name: {name}, config: {cfg}")
        
        required = ["table", "grain", "primary_date"]
        for r in required:
            if r not in cfg:
                raise ValueError(f"Fact '{name}' missing required field: {r}. Available fields: {list(cfg.keys())}")
        
        self.fact = Fact(
            name=name,
            table=cfg["table"],
            grain=cfg["grain"],
            primary_date=cfg["primary_date"],
        )
        
        print(f"[SUCCESS] Successfully loaded fact: {name}")


    def _load_dates(self):
        data = self._load_yaml("dates.yaml")
        for name, cfg in data.items():
            required = ["column", "sql", "supported_filters", "supported_grains"]
            for r in required:
                if r not in cfg:
                    raise ValueError(f"Date '{name}' missing required field: {r}")
            self.dates[name] = DateDimension(
                name=name,
                column=cfg["column"],
                sql=cfg["sql"],
                supported_filters=cfg["supported_filters"],
                supported_grains=cfg["supported_grains"],
            )

    def _load_dimensions(self):
        data = self._load_yaml("dimensions.yaml")
        for name, cfg in data.items():
            if "column" not in cfg or "type" not in cfg:
                raise ValueError(f"Dimension '{name}' must define column and type")
            self.dimensions[name] = Dimension(
                name=name,
                column=cfg["column"],
                type=cfg["type"],
            )

    def _load_measures(self):
        data = self._load_yaml("measures.yaml")
        for name, cfg in data.items():
            if "column" not in cfg and "sql" not in cfg:
                raise ValueError(f"Measure '{name}' must define column or sql")
            if "aggregation" not in cfg:
                raise ValueError(f"Measure '{name}' must define aggregation")
            
            self.measures[name] = Measure(
                name=name,
                column=cfg.get("column"),
                aggregation=cfg["aggregation"],
                mandatory_filters=cfg.get("mandatory_filters", []),
                sql=cfg.get("sql")
            )

    def _load_metrics(self):
        data = self._load_yaml("metrics.yaml")
        for name, cfg in data.items():
            if "expression" not in cfg or "behavior" not in cfg:
                raise ValueError(
                    f"Metric '{name}' must define expression and behavior"
                )
            self.metrics[name] = Metric(
                name=name,
                description=cfg.get("description", ""),
                expression=cfg["expression"],
                behavior=cfg["behavior"],
                filters=cfg.get("filters", []),  # NEW: Load metric-level filters
                min_safe_grain=cfg.get("min_safe_grain"),
            )

    def _basic_validate(self):
        if not self.fact:
            raise ValueError("No fact loaded")
        
        # Ensure primary date exists
        if self.fact.primary_date not in self.dates:
            raise ValueError(
                f"Primary date '{self.fact.primary_date}' not defined in dates.yaml"
            )
