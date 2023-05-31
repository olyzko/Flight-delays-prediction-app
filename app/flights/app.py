import datetime
from flask import Flask, request, render_template
from flask_sqlalchemy import SQLAlchemy
from geopy.distance import great_circle as grc
import regex as re
import pandas as pd
import pickle
import columns

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:pass@localhost/airport'
db = SQLAlchemy(app)


class Airports(db.Model):
    ID = db.Column(db.Integer, primary_key=True)
    NAME = db.Column(db.String)
    LATITUDE = db.Column(db.Float)
    LONGITUDE = db.Column(db.Float)


class Airlines(db.Model):
    AIRLINE_ID = db.Column(db.Integer, primary_key=True)
    AIRLINE_NAME = db.Column(db.String)


class Weather(db.Model):
    ID = db.Column(db.Integer, primary_key=True)
    MONTH = db.Column(db.Integer)
    DAY_OF_MONTH = db.Column(db.Integer)
    AIRPORT_ID = db.Column(db.Integer)
    PRCP = db.Column(db.Float)
    SNOW = db.Column(db.Float)
    SNWD = db.Column(db.Float)
    TMAX = db.Column(db.Float)
    TMIN = db.Column(db.Float)
    WDF2 = db.Column(db.Float)
    WSF2 = db.Column(db.Float)
    AWND = db.Column(db.Float)


class Airline_historical(db.Model):
    ID = db.Column(db.Integer, primary_key=True)
    AIRLINE_NAME = db.Column(db.String)
    MONTH = db.Column(db.Integer)
    AIRLINE_HISTORICAL = db.Column(db.Float)


class Airport_historical(db.Model):
    ID = db.Column(db.Integer, primary_key=True)
    DEP_AIRPORT_ID = db.Column(db.Integer)
    MONTH = db.Column(db.Integer)
    DEP_AIRPORT_HIST = db.Column(db.Float)
    ARR_AIRPORT_HIST = db.Column(db.Float)


class Day_historical(db.Model):
    ID = db.Column(db.Integer, primary_key=True)
    DAY_OF_WEEK = db.Column(db.Integer)
    MONTH = db.Column(db.Integer)
    DAY_HISTORICAL = db.Column(db.Float)


class Time_block_historical(db.Model):
    ID = db.Column(db.Integer, primary_key=True)
    TIME_BLOCK = db.Column(db.String)
    MONTH = db.Column(db.Integer)
    DEP_BLOCK_HIST = db.Column(db.Float)
    ARR_BLOCK_HIST = db.Column(db.Float)


def find_distance_group(distance):
    if distance < 250:
        return 1
    elif distance < 500:
        return 2
    elif distance < 750:
        return 3
    elif distance < 1000:
        return 4
    elif distance < 1250:
        return 5
    else:
        return 6


def find_time_block(time):
    res = "0001-0559"
    hour = int(time[:2])
    if hour < 6:
        return res
    else:
        res = re.sub("1", "0", res)
        res = re.sub("^..", str(hour) if hour >= 10 else f"0{str(hour)}", res)
        res = re.sub("(?<=-)..", str(hour) if hour >= 10 else f"0{str(hour)}", res)
    return res


@app.route('/', methods=['GET', 'POST'])
def main():
    airports = Airports.query.all()
    airlines = Airlines.query.all()

    # If a form is submitted
    if request.method == "POST":

        # Unpickle classifier
        clf = pickle.load(open('xgb_clf_pkl', 'rb'))

        # Get values through input bars
        year = request.form.get("year", type=int)
        month = request.form.get("month", type=int)
        day_of_month = request.form.get("day_of_month", type=int)
        input_date = datetime.date(year, month, day_of_month)
        day_of_week = input_date.weekday() + 1

        dep_airport_id = request.form.get("dep_airport", type=int)
        arr_airport_id = request.form.get("arr_airport", type=int)
        dep_airport = Airports.query.filter_by(ID=dep_airport_id).first()
        arr_airport = Airports.query.filter_by(ID=arr_airport_id).first()
        dep_time = request.form.get("dep_time")
        arr_time = request.form.get("arr_time")
        dep_delay = request.form.get("dep_delay", type=int)
        dep_delay15 = 0 if dep_delay < 15 else 1
        airline = request.form.get("airline")

        dep_airport_coords = (dep_airport.LATITUDE, dep_airport.LONGITUDE)
        arr_airport_coords = (arr_airport.LATITUDE, arr_airport.LONGITUDE)
        distance = grc(dep_airport_coords, arr_airport_coords).km
        distance_group = find_distance_group(distance)
        segment_number = 3.0
        dep_time_block = find_time_block(dep_time)
        arr_time_block = find_time_block(arr_time)

        weather_dep = Weather.query.filter_by(MONTH=month, DAY_OF_MONTH=day_of_month, AIRPORT_ID=dep_airport_id).first()
        weather_dep_dict = {"PRCP_DEP": weather_dep.PRCP, "SNOW_DEP": weather_dep.SNOW, "SNWD_DEP": weather_dep.SNWD,
                            "TMAX_DEP": weather_dep.TMAX, "TMIN_DEP": weather_dep.TMIN, "WDF2_DEP": weather_dep.WDF2,
                            "WSF2_DEP": weather_dep.WSF2, "AWND_DEP": weather_dep.AWND}
        weather_arr = Weather.query.filter_by(MONTH=month, DAY_OF_MONTH=day_of_month, AIRPORT_ID=arr_airport_id).first()
        weather_arr_dict = {"PRCP_ARR": weather_arr.PRCP, "SNOW_ARR": weather_arr.SNOW, "SNWD_ARR": weather_arr.SNWD,
                            "TMAX_ARR": weather_arr.TMAX, "TMIN_ARR": weather_arr.TMIN, "WDF2_ARR": weather_arr.WDF2,
                            "WSF2_ARR": weather_arr.WSF2, "AWND_ARR": weather_arr.AWND}

        airline_hist = Airline_historical.query.filter_by(MONTH=month, AIRLINE_NAME=airline).first().AIRLINE_HISTORICAL
        dep_airport_hist = Airport_historical.query.filter_by(MONTH=month,
                                                              DEP_AIRPORT_ID=dep_airport_id).first().DEP_AIRPORT_HIST
        arr_airport_hist = Airport_historical.query.filter_by(MONTH=month,
                                                              DEP_AIRPORT_ID=arr_airport_id).first().ARR_AIRPORT_HIST
        day_hist = Day_historical.query.filter_by(MONTH=month, DAY_OF_WEEK=day_of_week).first().DAY_HISTORICAL
        dep_time_block_hist = Time_block_historical.query.filter_by(MONTH=month,
                                                                    TIME_BLOCK=dep_time_block).first().DEP_BLOCK_HIST
        arr_time_block_hist = Time_block_historical.query.filter_by(MONTH=month,
                                                                    TIME_BLOCK=arr_time_block).first().ARR_BLOCK_HIST

        cols = columns.columns
        flight = pd.DataFrame([dep_delay, dep_delay15, distance, distance_group, segment_number]).T
        flight.columns = cols[:5]
        flight = pd.concat([flight, pd.DataFrame(weather_dep_dict, index=[0])], axis=1)
        flight = pd.concat([flight, pd.DataFrame(weather_arr_dict, index=[0])], axis=1)
        flight['CARRIER_HISTORICAL'] = airline_hist
        flight['DEP_AIRPORT_HIST'] = dep_airport_hist
        flight['ARR_AIRPORT_HIST'] = arr_airport_hist
        flight['DAY_HISTORICAL'] = day_hist
        flight['DEP_BLOCK_HIST'] = dep_time_block_hist
        flight['ARR_BLOCK_HIST'] = arr_time_block_hist

        for i in Airports.query.order_by(Airports.ID).all():
            flight = pd.concat([
                flight, pd.DataFrame(data=[0], columns=[f'DEP_AIRPORT_{i.ID}'])
            ], axis=1)
        for i in Airports.query.order_by(Airports.ID).all():
            flight = pd.concat([
                flight, pd.DataFrame(data=[0], columns=[f'ARR_AIRPORT_{i.ID}'])
            ], axis=1)

        for i in Time_block_historical.query.distinct(Time_block_historical.TIME_BLOCK).all():
            flight = pd.concat([
                flight, pd.DataFrame(data=[0], columns=[f'DEP_TIME_{i.TIME_BLOCK}'])
            ], axis=1)
        for i in Time_block_historical.query.distinct(Time_block_historical.TIME_BLOCK).all():
            flight = pd.concat([
                flight, pd.DataFrame(data=[0], columns=[f'ARR_TIME_{i.TIME_BLOCK}'])
            ], axis=1)

        flight[f'DEP_AIRPORT_{dep_airport_id}'] = 1
        flight[f'ARR_AIRPORT_{arr_airport_id}'] = 1
        flight[f'DEP_TIME_{dep_time_block}'] = 1
        flight[f'ARR_TIME_{arr_time_block}'] = 1

        print(flight.columns)

        # Get prediction
        prediction = clf.predict(flight)[0]

    else:
        prediction = ""

    return render_template("index.html", output=prediction, airports=airports, airlines=airlines)


if __name__ == '__main__':
    app.run()
