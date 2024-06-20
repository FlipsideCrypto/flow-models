import requests
import json
import os

def create_message(**kwargs):
    messageBody = {
        "text": f"Ad hoc :dbt: job run completed for :{os.environ.get('DATABASE').split('_DEV')[0]}: {os.environ.get('DATABASE')}",
        "attachments": [
            {
                "color": kwargs["color"],
                "fields": [
                    {
                        "title": "Environment",
                        "value": kwargs["environment"],
                        "short": True
                    },
                    {
                        "title": "Warehouse",
                        "value": kwargs["warehouse"],
                        "short": True
                    },
                    {
                        "title": "Elapsed Time",
                        "value": kwargs["elapsed_time"],
                        "short": True
                    },
                    {
                        "title": "Run status",
                        "value": f"{':check:' if kwargs['dbt_run_status'] == 'success' else ':x:'} {kwargs['dbt_run_status'].capitalize()}",
                        "short": True
                    },
                    {
                        "title": "dbt Command",
                        "value": kwargs["dbt_command"],
                        "short": False
                    }
                ]
            }
        ]
    }

    return messageBody


def send_alert(webhook_url, data):
    """Sends a message to a slack channel"""

    send_message = create_message(
        **data,
        color="#008080" if data["dbt_run_status"] == "success" else "#FF0000"
    )

    x = requests.post(webhook_url, json=send_message)


if __name__ == '__main__':
    data = {
        "environment": os.environ.get("ENVIRONMENT"),
        "database": os.environ.get("DATABASE"),
        "warehouse": os.environ.get("WAREHOUSE"),
        "dbt_command": os.environ.get("DBT_COMMAND"),
        "elapsed_time": os.environ.get("ELAPSED_TIME"),
        "dbt_run_status": os.environ.get("DBT_RUN_STATUS")
    }

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    send_alert(webhook_url, data)
