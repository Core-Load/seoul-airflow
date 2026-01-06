from airflow.models import Variable

import logging
import requests

def on_failure_callback(context):
    text = str(context['task_instance'])
    text += "```" + str(context.get('exception')) +"```"
    send_message_to_a_slack_channel(text, ":scream:")

def send_message_to_a_slack_channel(message, emoji):
    url = "https://hooks.slack.com/services/"+Variable.get("slack_url")
    headers = {
        'content-type': 'application/json',
    }
    data = { "username": "Check_DAG", "text": message, "icon_emoji": emoji }
    r = requests.post(url, json=data, headers=headers)
    return r