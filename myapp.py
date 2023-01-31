from flask import Flask, request, jsonify
from flask_restful import Resource, Api, reqparse
import pandas as pd
import ast
import datetime


app = Flask(__name__)
api = Api(app)

# getting the csv data in program
df = pd.read_csv("raw_data.csv") 

# code to sort data by 'sts'

sorted_data = df.sort_values(by=['sts'])

sd = sorted_data.reindex(columns=['sts', 'device_fk_id', 'latitude','longitude','time_stamp','speed'])
print(sd)


#code to get start location and end location for device id


grouped = df.groupby('device_fk_id')
result = grouped[['latitude', 'longitude']].agg(['first', 'last'])

result = result.reset_index()
result['start'] = list(zip(result['latitude']['first'], result['longitude']['first']))
result['end'] = list(zip(result['latitude']['last'], result['longitude']['last']))
result = result[['device_fk_id', 'start', 'end']]
result = result.rename(columns={'start': 'Start_Location_Coordinates', 'end': 'End_Location_Coordinates'})

# result = grouped[['time_stamp', 'latitude', 'longitude']].agg(['first', 'last'])



def convert_datetime(value):
    try:
        return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MM:SS")

app.url_map.converters['datetime'] = convert_datetime



@app.route('/device_latest/<int:device_id>', methods=["GET"])
def get_device(device_id):
    
 # Filter the data frame to get the data for the specified device
    device_data = df[df["device_fk_id"] == device_id]
    latitude = device_data.tail(1)[["latitude"]].values.tolist()
    longitude = device_data.tail(1)[["longitude"]].values.tolist()
    time_stamp = device_data.tail(1)[["time_stamp"]].values.tolist()
    server_time = device_data.tail(1)[["sts"]].values.tolist()
    speed = device_data.tail(1)[["speed"]].values.tolist()

    return jsonify({'device_fk_id': device_id,
    'latitude':latitude,'longitude':longitude,
    'time_stamp':time_stamp,'sts':server_time,
    'speed':speed})

@app.route('/device/<int:device_id>', methods=['GET'])

def get_device_data(device_id):
    device_data = result[result['device_fk_id'] == device_id]
    Start_Location = device_data[["Start_Location_Coordinates"]].values.tolist()
    End_Location = device_data[["End_Location_Coordinates"]].values.tolist()
    
    return jsonify({ 'device_fk_id': device_id,'Start Location': Start_Location,'End Location': End_Location})



@app.route('/location_points', methods=['GET'])
def location_points():

    # Get the device ID, start time, and end time from the request
    # device_id = request.args.get('device_id')
    # start_time = request.args.get('start_time')
    # end_time = request.args.get('end_time')
    
    # Filter the dataframe to only include rows for the specified device ID
    # and within the specified time range
    filtered_df = df[(df['device_fk_id'] == 24809)]
    
    # Create a list of dictionaries, where each dictionary represents one location point
    location_points = [{'latitude': row['latitude'],
                        'longitude': row['longitude'],
                        'time_stamp': row['time_stamp']}
                       for _, row in filtered_df.iterrows()]
    
    # Return the list of location points as a JSON response
    return jsonify(location_points)




if __name__ == "__main__":
    app.run(debug=True)




