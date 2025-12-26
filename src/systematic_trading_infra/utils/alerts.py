import requests

class PushoverAlters:

    @staticmethod
    def send_pushover(pushover_user:str,
                      pushover_token:str,
                      message: str,
                      title="Systematic Trading Infra"):
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "user": pushover_user,
                "token": pushover_token,
                "message": message,
                "title": title,
            },
            timeout=5
        )
        r.raise_for_status()
