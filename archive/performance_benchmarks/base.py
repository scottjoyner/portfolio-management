"""
Base Benchmark Classes and Utilities

Provides abstract base classes, metrics collection, and report generation.
"""
import time
import json
import html
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime


@dataclass
class BenchmarkResult:
    """Stores results from a single benchmark run."""
    name: str
    target: float  # Target value (e.g., max latency in ms)
    actual: float  # Actual measured value
    unit: str = "ms"
    passed: bool = True
    timestamp: datetime = field(default_factory=datetime.now)
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "target": self.target,
            "actual": self.actual,
            "unit": self.unit,
            "passed": self.passed,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details
        }
    
    def status_icon(self) -> str:
        return "✅ PASS" if self.passed else "❌ FAIL"


class MetricsCollector:
    """Collects and aggregates performance metrics."""
    
    def __init__(self):
        self.results: List[BenchmarkResult] = []
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def record(self, result: BenchmarkResult) -> None:
        """Record a benchmark result."""
        self.results.append(result)
    
    def add_result(self, name: str, target: float, actual: float,
                   unit: str = "ms", passed: bool = True,
                   details: Optional[Dict] = None) -> None:
        """Convenience method to add a result."""
        self.record(BenchmarkResult(
            name=name,
            target=target,
            actual=actual,
            unit=unit,
            passed=passed,
            details=details or {}
        ))
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        if not self.results:
            return {"error": "No results recorded"}
        
        passed_count = sum(1 for r in self.results if r.passed)
        failed_count = len(self.results) - passed_count
        overall_passed = passed_count > 0 and failed_count == 0
        
        return {
            "total": len(self.results),
            "passed": passed_count,
            "failed": failed_count,
            "overall_status": "✅ ALL PASSED" if overall_passed else "⚠️ SOME FAILED",
            "results": [r.to_dict() for r in self.results]
        }


class ReportGenerator:
    """Generates HTML and Markdown reports from benchmark results."""
    
    def __init__(self, title: str = "Performance Benchmark Results"):
        self.title = title
        self.collector = MetricsCollector()
    
    def generate_html(self) -> str:
        """Generate HTML report."""
        summary = self.collector.get_summary()
        results_data = json.dumps(summary["results"])
        
        # Build HTML template
        html_parts = [
            '<!DOCTYPE html>',
            '<html><head>',
            '    <meta charset="UTF-8">',
            f'    <title>{html.escape(self.title)}</title>',
            '    <style>',
            '        body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; background: #f5f5f5; }',
            '        .container { max-width: 900px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }',
            '        h1 { color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }',
            '        .summary { background: #f8f9fa; padding: 15px; border-radius: 4px; margin: 20px 0; }',
            '        .result-table { width: 100%; border-collapse: collapse; margin: 20px 0; }',
            '        .result-table th, .result-table td { padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6; }',
            '        .result-table th { background: #f8f9fa; font-weight: 600; }',
            '        .status-pass { color: #28a745; }',
            '        .status-fail { color: #dc3545; }',
            '    </style>',
            '</head><body>',
            '    <div class="container">',
            f'        <h1>{html.escape(self.title)}</h1>',
            '',
            '        <div class="summary">',
            f'            <strong>Summary:</strong> {html.escape(summary["overall_status"])}',
            '            <br><small>' + str(summary['passed']) + '/' + str(summary['total']) + ' tests passed</small>',
            '        </div>',
            '',
            '        <table class="result-table">',
            '            <thead>',
            '                <tr>',
            '                    <th>Name</th>',
            '                    <th>Target</th>',
            '                    <th>Actual</th>',
            '                    <th>Status</th>',
            '                </tr>',
            '            </thead>',
            '            <tbody>'
        ]
        
        # Add table rows
        for result in summary["results"]:
            status_class = "status-pass" if result["passed"] else "status-fail"
            row = '                <tr>'
            row += '\n                    <td>' + html.escape(result['name']) + '</td>\n'
            row += '                    <td>' + str(result['target']) + ' ' + result['unit'] + '</td>\n'
            row += '                    <td>' + str(result['actual']) + ' ' + result['unit'] + '</td>\n'
            row += '                    <td class="' + status_class + '">' + html.escape(result['status_icon']()) + '</td>\n                </tr>'
            html_parts.append(row)
        
        # Add closing tags
        html_parts.extend([
            '            </tbody>',
            '        </table>',
            '',
            f'        <p class="timestamp">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>',
            '    </div></body></html>'
        ]
        
        return "\n".join(html_parts)
    
    def generate_markdown(self) -> str:
        """Generate Markdown report."""
        summary = self.collector.get_summary()
        
        md_lines = [
            f"# {self.title}",
            "",
            f"## Summary",
            f"{summary['overall_status']}",
            f"- **Passed:** {summary['passed']} / {summary['total']}",
            "",
            f"## Results"
        ]
        
        for result in summary["results"]:
            status = "✅ PASS" if result["passed"] else "❌ FAIL"
            md_lines.append(f"- **{result['name']}**: {result['actual']}{result['unit']} (target: {result['target']}{result['unit']}) - {status}")
        
        return "\n".join(md_lines)


# Convenience function to run benchmarks and generate report
def run_benchmarks(benchmark_functions: List[callable], title: str = "Benchmark Results") -> ReportGenerator:
    """
    Run a list of benchmark functions and collect results.
    
    Each benchmark function should:
    - Accept optional parameters
    - Return a tuple (name, target, actual, passed, details)
    """
    generator = ReportGenerator(title)
    collector = generator.collector
    
    for func in benchmark_functions:
        try:
            result = func()
            if isinstance(result, tuple) and len(result) >= 4:
                name, target, actual, passed = result[:4]
                details = result[4] if len(result) > 4 else {}
                collector.add_result(name, target, actual, passed=passed, details=details)
        except Exception as e:
            # Record failed benchmark
            collector.add_result(
                name=f"<error: {func.__name__}",
                target=0,
                actual=float("inf"),
                passed=False,
                details={"error": str(e)}
            )
    
    return generator
