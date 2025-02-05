import random
from datetime import datetime
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from prefect import flow, task


# Task to connect to GIS
@task
def connect_to_gis():
    gis = GIS("https://geospatial-stg.aecomonline.net/portal", "portaladmin", "Macs2023")
    return gis


# Task to define the feature layer
@task
def define_feature_layer(gis):
    feature_layer_url = "https://geospatial-stg.aecomonline.net/server/rest/services/Hosted/Append_Test/FeatureServer/0"
    layer = FeatureLayer(feature_layer_url)
    return layer


# Task to generate random records
@task
def generate_random_records(record_count):
    features = []
    for _ in range(record_count):
        x = random.uniform(-20000000, 20000000)  # Random x coordinate
        y = random.uniform(-10000000, 10000000)  # Random y coordinate
        features.append({
            "attributes": {
                "name": f"Name_{random.randint(1, 10000)}",
                "city": f"City_{random.randint(1, 10000)}"
            },
            "geometry": {
                "x": x,
                "y": y,
                "spatialReference": {"wkid": 102100}
            }
        })
    return features


# Task to prepare edits data
@task
def prepare_edits_data(record_count, features):
    edits_data = {
        "layers": [
            {
                "layerDefinition": {
                    "id": 0,
                    "name": "Append Test",
                    "type": "Feature Layer",
                    "geometryType": "esriGeometryPoint",
                    "hasM": False,
                    "hasZ": False,
                    "fields": [
                        {"name": "objectid", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False},
                        {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "globalid", "nullable": False},
                        {"name": "name", "type": "esriFieldTypeString", "alias": "Name", "nullable": True,
                         "length": 256},
                        {"name": "city", "type": "esriFieldTypeString", "alias": "City", "nullable": True,
                         "length": 256}
                    ]
                },
                "featureSet": {
                    "geometryType": "esriGeometryPoint",
                    "spatialReference": {"wkid": 102100},
                    "features": features
                }
            }
        ]
    }
    return edits_data


# Task to print detailed status
@task
def print_status(status, status_url):
    submission_time = status.get("submissionTime")  # In milliseconds
    last_updated_time = status.get("lastUpdatedTime")  # In milliseconds

    print(f"Status URL: {status_url}")
    if submission_time:
        submission_dt = datetime.utcfromtimestamp(submission_time / 1000)
        print(f"Submission Time: {submission_dt.strftime('%Y-%m-%d %H:%M:%S.%f UTC')}")
    if last_updated_time:
        last_updated_dt = datetime.utcfromtimestamp(last_updated_time / 1000)
        print(f"Last Updated Time: {last_updated_dt.strftime('%Y-%m-%d %H:%M:%S.%f UTC')}")

    print(f"Job Status: {status.get('status', 'Unknown')}")
    print(f"Record Count: {status.get('recordCount', 'Unknown')}")
    print(f"Layer Name: {status.get('layerName', 'Unknown')}")

    # Calculate and display the time taken
    if submission_time and last_updated_time:
        time_taken_ms = last_updated_time - submission_time
        if time_taken_ms > 1000:
            time_taken_sec = time_taken_ms / 1000
            print(f"Append operation took {time_taken_sec:.2f} seconds.")
        else:
            print(f"Append operation took {time_taken_ms} milliseconds.")


# Task to perform the append operation
@task
def perform_append_operation(layer, edits_data):
    try:
        result = layer.append(
            edits=edits_data,
            upload_format="featureCollection",
            rollback=True,
            return_messages=True
        )

        if isinstance(result, tuple):
            status = result[1]
            status_url = status.get("statusUrl", "No status URL available")
            return status, status_url
        else:
            print("Unexpected result format:", result)
            return None, None
    except Exception as e:
        print(f"Error during append: {e}")
        return None, None


# Define the flow
@flow
def gis_append_flow(record_count=100):
    gis = connect_to_gis()
    layer = define_feature_layer(gis)
    features = generate_random_records(record_count)
    edits_data = prepare_edits_data(record_count, features)
    status, status_url = perform_append_operation(layer, edits_data)

    if status and status_url:
        print_status(status, status_url)


# Run the flow
if __name__ == "__main__":
    gis_append_flow(record_count=100)