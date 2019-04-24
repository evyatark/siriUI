var mymap = 0;
var mapAllRoutesDisplayed = new Map();
var allTripsFromJs = []; // initialized in solution.html ready function, to content of allTrips

const log = false;

function displayRouteOnMap(shape, color1) {
    console.log('displaying shape...  (' + shape.length + ' points)');
    const polyline = L.polyline(shape, {color: color1}).addTo(mymap);
    console.log('polyline added to map');
    mymap.fitBounds(polyline.getBounds());
    return polyline;
}


function displaySiriPointsOnMap(siriPoints, iconFileName) {
    // siriPoints is an array of objects like this:
    /*
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    32.175350189208984,
                    34.891281127929695
                ]
            },
            "properties": {
                "time_recorded": "07:02:25"
            }
        },
        */
    if (!iconFileName) {
        iconFileName = 'red-pin.svg';
    }
    let iconYellow = L.icon({
        iconUrl: './icons/' + iconFileName,
        iconSize: 30
    });
    const siriMarkers = siriPoints.map(siriPoint => {
            const coordinates = siriPoint.geometry.coordinates;
            const title = "time:" + siriPoint.properties.time_recorded;
            const marker = L.marker(coordinates, {
                icon: iconYellow,
                title: title,
                riseOnHover: true
            }).addTo(mymap).bindPopup("<b>time:" + siriPoint.properties.time_recorded + "</b>");
            // marker.on('mouseover', function(e) {
            //     let sp = siriPoint.properties;
            //     parent.document.getElementById('siri_current_time').value = 'Time: '+sp.time_recorded;
            //     parent.document.getElementById('siri_line_name').value = "קו 149";//sp.properties.missing;
            //     parent.document.getElementById('siri_vehicle_id').value = "vehicle Id: 3174039";//sp.properties.missing;
            //     parent.document.getElementById('siri_departure').value = "יצא ב 06:14";//sp.properties.missing;
            //     parent.document.getElementById('siri_estimated_arrival').value = "אמור להגיע ב 07:14";//sp.properties.missing;
            // }).on('mouseout', clearSiriPointDisplay);
            return marker;
        }
        )
    ;
    return siriMarkers;
}

function clearSiriPointDisplay(event) {
    parent.document.getElementById('siri_current_time').value = "";
    parent.document.getElementById('siri_line_name').value = "";
    parent.document.getElementById('siri_vehicle_id').value = "";
    parent.document.getElementById('siri_departure').value = "";
    parent.document.getElementById('siri_estimated_arrival').value = "";
}

function displayStopsOnMap(stops) {
    // stops is array of objects like this:
    /*
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        32.183167,
                        34.928792
                    ]
                },
                "properties": {
                    "stop_code": "37065",
                    "stop_id": "24431"
                }
            },
    */
    let iconYellow = L.icon({
        iconUrl: '/home/evyatar/work/hasadna/Hackathon2019/icons/yellow-flag.svg',
        iconSize: 30
    });
    const stopsMarkers = stops.map(stop => {

            const coordinates = stop.geometry.coordinates;
            const marker = L.marker(coordinates, {icon: iconYellow}).addTo(mymap).bindPopup("<b>code:" +
                stop.properties.stop_code +
                "</b><br>id:" +
                stop.properties.stop_id);
            marker.on('mouseover', function (e) {
                let x = parent.document.getElementById('gtfs_stop_code');
                // gtfs_stop_code in the parent document -
                //works in Firefox but not in Chrome
                // in Chrome use https://www.thegeekstuff.com/2016/09/disable-same-origin-policy/
                //x.placeholder=stop.properties.stop_code;      // possible to change placeholder text
                parent.document.getElementById('gtfs_stop_code').value = stop.properties.stop_code;
                parent.document.getElementById('gtfs_stop_id').value = stop.properties.stop_id;
                parent.document.getElementById('gtfs_stop_name').value = "תחנה בזמן";//stop.properties.stop_name;
            }).on('mouseout', clearStopDisplay);
            return marker;
        }
    );
    return stopsMarkers;
}

function clearStopDisplay(event) {
    parent.document.getElementById('gtfs_stop_code').value = "";
    parent.document.getElementById('gtfs_stop_id').value = "";
    parent.document.getElementById('gtfs_stop_name').value = "";

}

function populateTripsGrid(allTrips) {
    const xx = parent.document.getElementById('all_lines');
    // xx.innerHTML = "<tr class=\"clickable-row\"><td>7:20</td><td>tripId</td><td>vehicleNumber</td></tr>" + xx.innerHTML;
    const trips = allTrips.gtfsTrips.reverse();
    for (let i = 0; i < trips.length; i++) {
        let trip = trips[i];
        let tripId = trip.siriTripId;
        let vid = trip.vehicleId;
        let oad = trip.originalAimedDeparture;
        // generate td
        let td = "<tr class=\"clickable-row\"><td>" + oad + "</td><td>" + tripId + "</td><td>" + vid + "</td></tr>";
        xx.innerHTML = td + xx.innerHTML;
    }
}

function askDisplayAll(tripId) {
    console.log("value of allTripsFromJs is:" + allTripsFromJs);
    const gtfsTrip = allTripsFromJs.gtfsTrips.find(gTrip => gTrip.siriTripId == tripId);
    console.log("found gtfsTrip for tripId=" + tripId);
    const route1 = displayAll(gtfsTrip, 'black');
    console.log("add route to map by tripId...")
    mapAllRoutesDisplayed.set(tripId, route1);
    console.log("added. map now contains " + mapAllRoutesDisplayed.size);

}

function displayAll(tripObject, color) {
    if (!color) {
        color = 'red';      // color of the markers
    }
    let polyline = {};
    if (tripObject.shape) {
        polyline = displayRouteOnMap(tripObject.shape.coordinates, 'red');    // color of the polyline
    }
    //const stopsMarkers = displayStopsOnMap(tripObject.stops);
    const siriMarkers = displaySiriPointsOnMap(tripObject.siri.features, color + '-pin.svg');   // color of the markers
    return {
        route: {}, // polyline,
        stops: {}, //stopsMarkers,
        siri: siriMarkers
    };
}

function removeTripFromMap(tripId) {
    removeAll(mapAllRoutesDisplayed.get(tripId));
    console.log("remove tripId " + tripId);
}

function removeAll(routeObject) {
    if (routeObject) {
        console.log("removing route " + routeObject);
        routeObject.route.removeFrom(mymap);   //: polyline,
        routeObject.stops.forEach(stop => stop.removeFrom(mymap));   //: stopsMarkers,
        routeObject.siri.forEach(point => point.removeFrom(mymap));    //: siriMarkers
    }
}