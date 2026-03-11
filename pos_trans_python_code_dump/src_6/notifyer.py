from js import fetch, JSON

async def send_slack_alert(env, payload):
    if not env.SLACK_WEBHOOK:
        return

    message = {
        "text": f"""
🚨 POS Processing Failure
Batch ID: {payload.get("batch_id")}
Type: {payload.get("type")}
Error: {payload.get("error")}
Retries: {payload.get("retry_count")}
"""
    }

    await fetch(env.SLACK_WEBHOOK, {
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "body": JSON.stringify(message)
    })


async def send_email_alert(env, subject, message):
    await fetch("https://api.sendgrid.com/v3/mail/send", {
        "method": "POST",
        "headers": {
            "Authorization": f"Bearer {env.SENDGRID_API_KEY}",
            "Content-Type": "application/json"
        },
        "body": JSON.stringify({
            "personalizations": [{
                "to": [{"email": "ops@yourcompany.com"}]
            }],
            "from": {"email": "noreply@yourcompany.com"},
            "subject": subject,
            "content": [{
                "type": "text/plain",
                "value": message
            }]
        })
    })
