import pandas as pd
from itables.javascript import to_html_datatable

def display_data(header:list[str], rows: list):
    """
    Display data in a rich interactive format similar to Databricks

    Args:
        header: List of column names
        rows: List of data rows
    """
    # Convert to pandas DataFrame for easier manipulation
    df = pd.DataFrame(rows, columns=header)

    # Return a display-data formatted result
    return {
        "data": {
            "text/html": to_html_datatable(df),
            "text/plain": _format_text_table(header, rows)
        },
        "metadata": {
            "application/json": {
                "rowCount": len(rows)
            }
        }
    }

def _generate_table_headers(df):
    """Generate HTML for table headers"""
    headers = []
    for col in df.columns:
        headers.append(f'<th class="sortable">{col}</th>')
    return ''.join(headers)

def _format_text_table(header, rows):
    """Format data as plain text table using tabulate"""
    from tabulate import tabulate
    return tabulate(rows, headers=header)
