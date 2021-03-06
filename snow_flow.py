import datetime
import pendulum
import requests
from prefect import task, Flow, Parameter
from prefect.engine.signals import SKIP
from prefect.environments.storage import Docker
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.notifications.slack_task import SlackTask
from prefect.tasks.secrets import Secret


city = Parameter(name="City", default="Alpine Meadows")
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
def is_snowing_this_week(data):
    """
    Given a list of hourly forecasts, returns a boolean specifying
    whether there is snow in this week's forecast.
    """
    snow = [
        forecast["snow"].get("3h", 0) for forecast in data["list"] if "snow" in forecast
    ]
    if not sum([s >= 1 for s in snow]) >= 8:
        raise SKIP("There is not much snow in the forecast.")


notification = SlackTask(
    message="There is snow in the forecast for this week - it might be time to hit the slopes!",
    webhook_secret="DAVID_SLACK_URL",
)


storage = Docker(registry_url="joshmeek18", image_name="flows")

with Flow("Snow Flow", storage=storage, schedule=Schedule(
        clocks=[CronClock("0 18 * * 1-5", start_date=pendulum.now(tz="US/Pacific"))],
    )) as flow:
    forecast = pull_forecast(city=city, api_key=api_key)
    snow = is_snowing_this_week(forecast)
    notification.set_upstream(snow)

flow.register(project_name="Snow Flow")
