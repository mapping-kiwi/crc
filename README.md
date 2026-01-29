# Wildfire Evacuation Data Pipeline

ETL pipeline for scraping, cleaning, and enriching wildfire evacuation data with census demographics. This pipeline currently scrapes Tier 1 government data from Manitoba. 

## Features
- Scrapes evacuation data from Manitoba government sources
- Fetches census data from Statistics Canada API
- Fuzzy matches local authorities to census geographies
- Enriches evacuation records with population and Indigenous demographics

## Installation

### Prerequisites
- Python 3.11 or higher
- Internet connection (for scraping and API access)

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/mapping-kiwi/crc.git
cd crc/mycode
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Run the pipeline**
```bash
python3 pipeline.py
```

## Usage

### Basic Usage
```bash
python3 pipeline.py
```

### Advanced Options
```bash
# Use custom match threshold
python3 pipeline.py --cutoff 85

# Skip scraping and use existing data
python3 pipeline.py --skip-scraping
```

## Outputs

The pipeline generates:
- `csv files/T1_Wildfire_Evacs_Enriched.csv` - Main enriched dataset
- `qa_reports/QA_Pipeline_[timestamp].txt` - Quality assurance report
- `csv files/unmatched_authorities.csv` - Authorities requiring manual review

## Google Colab (or environments of similar structure)

To run in Google Colab:
```python
# Clone repository
!git clone https://github.com/mapping-kiwi/crc.git
%cd crc/mycode

# Install dependencies
!pip install -q -r requirements.txt

# Run pipeline
!python3 pipeline.py
```

## Project Structure
```
mycode/
├── pipeline.py              # Main pipeline orchestrator
├── statscan_api.py         # Stats Canada API integration
├── io_paths.py             # File path management
├── requirements.txt        # Python dependencies
├── pipeline/
│   ├── extract/            # Data extraction modules
│   ├── transform/          # Data cleaning and matching
│   └── load/               # Export and reporting
└── csv files/              # Output data files
```

## Troubleshooting

**Import errors**: Make sure all dependencies are installed:
```bash
pip install -r requirements.txt
```

**Low match rates**: Check `csv files/unmatched_authorities.csv` for communities that need manual mapping.

**API failures**: The pipeline will use a fallback dataset with 33 Manitoba communities if the Stats Canada API is unavailable.
