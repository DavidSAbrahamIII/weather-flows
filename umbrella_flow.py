import datetime
import pendulum
import requests
from prefect import task, Flow, Parameter
from prefect.engine.signals import SKIP
from prefect.tasks.notifications.slack_task import SlackTask
from prefect.tasks.secrets import Secret


city = Parameter(name="City", default="San Jose")
api_key = Secret("WEATHER_API_KEY")


@task(max_retries=2, retry_delay=datetime.timedelta(seconds=5))
def pull_forecast(city, api_key):
    """
    Extract the 5-day 3-hour forecast for the provided City.
    """
    base_url = "http://api.openweathermap.org/data/2.5/forecast?"
    url = base_url + "appid=" + api_key + "&q=" + city
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    return data


@task
def is_raining_tomorrow(forecast):
    """
    Given a list of hourly forecasts, returns a boolean specifying
    whether there is rain in tomorrow's forecast.
    """
    pendulum.now("utc").add(days=1).strftime("%Y-%m-%d")
    rain = [
        w
        for forecast in data["list"]
        for w in forecast["weather"]
        if w["main"] == "Rain" and forecast["dt_txt"].startswith(tomorrow)
    ]
    if not bool(rain):
        raise SKIP("There is no rain in the forecast for tomorrow.")


notification = SlackTask(
    message="There is rain in the forecast for tomorrow - better take your umbrella out!",
    webhook_secret="DAVID_SLACK_URL",
)


with Flow("Umbrella Flow") as flow:
    forecast = pull_forecast(city=city, api_key=api_key)
    rain = is_raining_tomorrow(forecast)
    notification.set_upstream(rain)
