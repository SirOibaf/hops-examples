from flask import Flask, request, render_template
import hsfs
import pymysql
import requests
import json

app = Flask(__name__)
api_key = (
    "Hrd8uIuceA5DPI45.ufdBYRQWTeVPNt0wQ6donl15ug15JlgypEtrjU6SeSPdCx9L7Cm63XLI09RPU7jI"
)
instance = "50d40db0-27f4-11eb-a729-b3a0388357f2.cloud.hopsworks.ai"
project = "dataai"

feature_vector_query = ""
connection = None

@app.route("/")
def hello():
    return render_template("index.html")


@app.route("/value", methods=["POST"])
def value():
    vector = get_vector(request.form)
    result = send_request(vector)
    return render_template("index.html", value="Your property is valued: " + str(result) + "$")


def get_vector(request):
    conditions = [
        "`fg0`.`health_area`=%s",
        "`fg0`.`building_class`=%s",
        "`fg0`.`school_dist`=%s",
        "`fg0`.`health_area`=%s",
        "`fg0`.`police_prct`=%s",
        "`fg0`.`owner_type`=%s",
    ]

    query = feature_vector_query + " WHERE " + " AND ".join(conditions)

    try:
       with connection.cursor() as cursor:
           # Read a single record
           cursor.execute(
               query,
               (
                   request["health_area"],
                   request["building_class"],
                   request["school_dist"],
                   request["health_area"],
                   request["police_prct"],
                   request["owner_type"],
               ),
           )
           result = cursor.fetchone()
    except Exception as e:
       print(e)

    result["is_single_unit"] = 1 if request["number_of_units"] == 1 else 0
    result["is_large_residential"] = 1 if int(request["number_of_units"]) > 100 else 0
    result["is_owner_company"] = 1 if request["owner_type"] == "C" else 0
    result["is_owner_organization"] = 1 if request["owner_type"] == "O" else 0
    result["is_owner_private"] = 1 if request["owner_type"] == "P" else 0
    result["age_at_scale"] = request["age_at_sale"]
    result["has_garage_area"] = 1 if float(request["garage_area"]) > 0 else 0
    result["garage_area"] = request["garage_area"]

    return result

def send_request(result):
    inference_data = {"inputs": [[v for v in result.values()]]}

    headers = {"Authorization": "ApiKey " + api_key}

    response = requests.post(
        "https://50d40db0-27f4-11eb-a729-b3a0388357f2.cloud.hopsworks.ai:443/hopsworks-api/api/project/120/inference/models/RealEstateServing:predict",
        data=json.dumps(inference_data),
        headers=headers,
    )
    return abs(response.json()['predictions'][0])


if __name__ == "__main__":
    conn = hsfs.connection(
        instance, 443, project, api_key_value=api_key, hostname_verification=True
    )

    fs = conn.get_feature_store()
    td = fs.get_training_dataset("real_estate_price")

    feature_vector_query = td.get_query(online=True, with_label=False)

    connection = pymysql.connect(
        host="50d40db0-27f4-11eb-a729-b3a0388357f2.cloud.hopsworks.ai",
        user="dataai_meb10179",
        password="dxZhaOXnEDCetWazEbWVmpXAFcTdNRdZ",
        db="dataai",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    app.run()
