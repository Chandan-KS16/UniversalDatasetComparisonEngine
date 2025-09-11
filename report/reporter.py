# backend/report/reporter.py
from jinja2 import Template
import json
from typing import Dict, Any

HTML_TEMPLATE = """
<html>
<head><title>Dataset Comparison Report</title>
<style>
body{font-family: Arial, sans-serif; margin: 20px;}
h1{color:#222}
table{border-collapse: collapse; width: 100%;}
th, td{border:1px solid #ddd; padding:8px;}
th{background:#f4f4f4;}
.code{font-family: monospace; background:#fafafa; padding:6px; display:block;}
</style>
</head>
<body>
<h1>Dataset Comparison Report</h1>
<h2>Summary</h2>
<p>{{ summary }}</p>
<h3>Schema Comparison</h3>
<pre class="code">{{ schema | tojson(indent=2) }}</pre>
<h3>Data Comparison</h3>
<pre class="code">{{ data | tojson(indent=2) }}</pre>
</body>
</html>
"""

class ReportGenerator:
    def render_html(self, report: Dict[str, Any], path: str):
        tpl = Template(HTML_TEMPLATE)
        html = tpl.render(summary=report.get("summary", ""), schema=report.get("schema_comparison", {}), data=report.get("data_comparison", {}))
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

    def to_json(self, report: Dict[str, Any]) -> str:
        return json.dumps(report, default=str, indent=2)
