"""
Pytest configuration for HTML report generation.

Generates timestamped HTML reports in the reports/ directory.
"""

import pytest
from datetime import datetime
from pathlib import Path


def pytest_configure(config):
    """
    Configure pytest to generate timestamped HTML reports.
    """
    # Create reports directory
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = reports_dir / f"test_report_{timestamp}.html"
    
    # Set the HTML report path
    config.option.htmlpath = str(report_path)
    config.option.self_contained_html = True


def pytest_html_report_title(report):
    """
    Customize the HTML report title.
    """
    report.title = "Medallion Architecture Volume Tests"
