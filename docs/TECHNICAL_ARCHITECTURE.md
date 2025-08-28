# Technical Architecture

## System Overview

The CandidateFilings.com pipeline is a Python-based ETL (Extract, Transform, Load) system designed to process political candidate data from multiple US states. The system follows a modular, pipeline-based architecture with clear separation of concerns.

## Core Components

### 1. Main Pipeline Orchestrator

**File**: `src/pipeline/main_pipeline.py`

**Responsibilities**:
- Coordinate all processing steps
- Manage file lifecycle
- Handle error recovery
- Coordinate data flow between components

**Key Methods**:
```python
class MainPipeline:
    def run_full_pipeline(self) -> bool
    def run_state_cleaners(self) -> Dict[str, str]
    def run_office_standardization(self, cleaned_files: Dict[str, str]) -> str
    def run_national_standardization(self, office_standardized_file: str) -> str
    def run_deduplication(self, nationally_standardized_file: str) -> str
    def run_data_audit(self, deduplicated_file: str) -> str
    def upload_to_database(self, final_file: str) -> bool
```

**Design Patterns**:
- **Pipeline Pattern**: Sequential data processing stages
- **Strategy Pattern**: Pluggable state cleaners
- **Factory Pattern**: Dynamic state cleaner instantiation

### 2. State Cleaner Framework

**Location**: `src/pipeline/state_cleaners/`

**Architecture**:
- **Base Interface**: Each cleaner implements a standard interface
- **State-Specific Logic**: Custom cleaning rules per state
- **Consistent Output**: All cleaners produce standardized schema

**Required Methods**:
```python
def clean_[state]_candidates(input_file: str, output_dir: str = None) -> pd.DataFrame
```

**Common Processing Steps**:
1. Election data processing
2. Office and district standardization
3. Candidate name parsing
4. Party name standardization
5. Contact information cleaning
6. Address parsing and standardization
7. Final validation and cleanup

### 3. Office Standardization Engine

**File**: `src/pipeline/office_standardizer.py`

**Algorithm**:
- **Fuzzy Matching**: Uses similarity algorithms for office name matching
- **Category Classification**: Groups offices into standardized categories
- **Confidence Scoring**: Provides confidence levels for matches

**Categories**:
- US_HOUSE, US_SENATE, US_PRESIDENT
- STATE_HOUSE, STATE_SENATE, GOVERNOR
- COUNTY_COMMISSION, CITY_COUNCIL
- SCHOOL_BOARD, MAYOR
- OTHER_LOCAL_OFFICE

### 4. Database Management Layer

**File**: `src/config/database.py`

**Features**:
- **Connection Pooling**: Efficient database connection management
- **SSL Support**: Secure connections to Supabase
- **Error Handling**: Graceful connection failure handling
- **Transaction Management**: ACID compliance for data integrity

**Tables**:
```sql
-- Staging table for intermediate processing
staging_candidates (
    id SERIAL PRIMARY KEY,
    election_year INTEGER,
    office VARCHAR(255),
    candidate_name VARCHAR(255),
    -- ... other fields
);

-- Production table for final data
filings (
    id SERIAL PRIMARY KEY,
    stable_id VARCHAR(255) UNIQUE,
    -- ... other fields
);
```

## Data Flow Architecture

### Stage 1: Data Extraction
```
Raw Files → File Detection → Multi-File Merging → State Cleaner Input
```

**Multi-File Support**:
- Automatic detection of multiple files per state
- Intelligent merging based on file timestamps
- Support for different file formats (.xlsx, .csv, .xls)

### Stage 2: State-Level Processing
```
State Cleaner → Data Validation → Schema Standardization → Output File
```

**Processing Features**:
- State-specific address parsing
- Office name cleaning
- Party name standardization
- Contact information validation

### Stage 3: National Standardization
```
Combined Data → Office Standardization → Party Standardization → Address Standardization
```

**Standardization Rules**:
- Office names: Categorized into standard groups
- Party names: Major parties standardized, third parties preserved
- Addresses: Parsed into city, county, zip components

### Stage 4: Data Quality & Deduplication
```
Standardized Data → Quality Audit → Duplicate Detection → Clean Dataset
```

**Quality Checks**:
- Column consistency validation
- Data type verification
- Address field analysis
- Record completeness assessment

### Stage 5: Database Loading
```
Clean Dataset → Staging Table → Validation → Production Table
```

**Loading Strategy**:
- Staging table for validation
- Production table for final data
- Conflict resolution for duplicates

## Error Handling & Recovery

### Error Categories
1. **File I/O Errors**: Missing files, permission issues
2. **Data Processing Errors**: Invalid data formats, parsing failures
3. **Database Errors**: Connection issues, constraint violations
4. **Memory Errors**: Large dataset processing issues

### Recovery Strategies
- **Graceful Degradation**: Continue processing other states if one fails
- **Retry Logic**: Automatic retry for transient failures
- **Logging**: Comprehensive error logging for debugging
- **Cleanup**: Automatic cleanup of partial results

### Monitoring & Alerting
- **Progress Tracking**: Real-time processing status
- **Error Reporting**: Detailed error logs with context
- **Performance Metrics**: Processing time and throughput tracking

## Performance Optimization

### Memory Management
- **Data Sampling**: Use samples for large dataset audits
- **Streaming Processing**: Process data in chunks where possible
- **Garbage Collection**: Explicit cleanup of large DataFrames

### Processing Optimization
- **Parallel Processing**: Concurrent state processing (future enhancement)
- **Efficient Algorithms**: Optimized string matching and parsing
- **Caching**: Cache frequently accessed data structures

### File I/O Optimization
- **Batch Operations**: Group file operations where possible
- **Format Selection**: Choose optimal file formats for data types
- **Compression**: Use compressed formats for large datasets

## Security Considerations

### Data Protection
- **Input Validation**: Sanitize all input data
- **SQL Injection Prevention**: Use parameterized queries
- **Access Control**: Environment variable-based configuration

### Network Security
- **SSL/TLS**: Encrypted database connections
- **Connection Timeouts**: Prevent hanging connections
- **IP Restrictions**: Database access restrictions (if applicable)

## Scalability & Extensibility

### Horizontal Scaling
- **Modular Design**: Easy to add new states
- **Plugin Architecture**: Pluggable processing components
- **Configuration-Driven**: Minimal code changes for new features

### Performance Scaling
- **Batch Processing**: Configurable batch sizes
- **Memory Optimization**: Efficient data structures
- **Parallel Processing**: Future enhancement for concurrent processing

### Data Volume Scaling
- **Chunked Processing**: Handle datasets larger than memory
- **Streaming**: Process data without loading entire files
- **Database Partitioning**: Partition large tables by state or year

## Testing & Quality Assurance

### Testing Strategy
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end pipeline testing
- **Data Validation**: Output quality verification
- **Performance Tests**: Throughput and memory usage testing

### Quality Metrics
- **Data Completeness**: Percentage of complete records
- **Data Accuracy**: Validation against known good data
- **Processing Performance**: Time and resource usage
- **Error Rates**: Frequency and types of processing errors

## Deployment & Operations

### Environment Requirements
- **Python 3.8+**: Modern Python features support
- **Memory**: Minimum 8GB RAM for large datasets
- **Storage**: Sufficient space for data files and outputs
- **Network**: Stable internet connection for database access

### Monitoring
- **Log Analysis**: Regular log review and analysis
- **Performance Tracking**: Monitor processing times and resource usage
- **Error Tracking**: Track and resolve recurring issues
- **Data Quality**: Regular data quality assessments

### Maintenance
- **Log Rotation**: Automatic log file management
- **File Cleanup**: Regular cleanup of temporary files
- **Database Maintenance**: Regular database optimization
- **Dependency Updates**: Keep dependencies current and secure
