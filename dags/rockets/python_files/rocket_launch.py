import json
import requests


def _get_pictures():
    with open("/opt/airflow/dags/rockets/launches/launches.json", "r") as file:
        launches = json.load(file)

        image_urls = []
        for launch in launches["results"]:
            image_urls.append(launch["image"])

        for image_url in image_urls:
            response = requests.get(image_url)

            image_filename = image_url.split("/")[-1]
            target_file = f"/opt/airflow/dags/rockets/images/{image_filename}"

            with open(target_file, "wb") as image:
                image.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")