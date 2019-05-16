//var mymap ;   - declared in script in solution.html
var mapAllRoutesDisplayed = new Map();
var allTripsFromJs ; // initialized in solution.html ready function, to content of allTrips

const log = true;
function clog(arg) {
    if (log) {
        console.log(arg);
    }
}

function displayRouteOnMap(shape, color1) {
    clog('displaying shape...  (' + shape.length + ' points)');
    // mymap is declared in script in solution.html
    const polyline = L.polyline(shape, {color: color1}).addTo(mymap);
    clog('polyline added to map');
    //     mymap.fitBounds(polyline.getBounds());
    return polyline;
}


function displaySiriPointsOnMap(siriPoints, iconFileName) {
    // mymap is declared in script in solution.html
    clog("displaySiriPointsOnMap: mymap=" + mymap);
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
    clog("siriPoints array has size " + siriPoints.length);
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


// arg gtfsTripObject is an object of the format in allTrips
function askDisplayAll(gtfsTripObject, setView) {
    // mymap was declared as var in script in solution.html
    let tripId = gtfsTripObject.siriTripId;
    clog("asked to display trip with siri tripId=" + tripId);
    let shapeJson = sessionStorage.getItem("shapeOfSelectedRoute");
    if (!shapeJson) {   // shape null or empty?
        clog("warning: shape of route " + gtfsTripObject.routeId + " not found in sessionStorage (retrieved by tripsTable.js)");
    }
    else {
        gtfsTripObject.shape = { "coordinates": JSON.parse(shapeJson) } ;   // temporary?
    }
    const route1 = displayAll(mymap, gtfsTripObject, 'black');
    clog("add route to map by tripId...");
    mapAllRoutesDisplayed.set(tripId, route1);
    clog("added. map now contains " + mapAllRoutesDisplayed.size);
    if (setView) {
        mymap.setView(gtfsTripObject.siri.features[0].geometry.coordinates, 12);
    }
    //mymap.setView( [31.738494, 34.995529], 14);
}

function displayAll(mymap, tripObject, color) {
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
    if (tripId) {
        removeAll(mapAllRoutesDisplayed.get(tripId));
        clog("remove tripId " + tripId);
    }
}

function removeAll(routeObject) {
    if (routeObject) {
        clog("removing route " + routeObject);
        //routeObject.route.removeFrom(mymap);   //: polyline,
        //routeObject.stops.forEach(stop => stop.removeFrom(mymap));   //: stopsMarkers,
        routeObject.siri.forEach(point => point.removeFrom(mymap));    //: siriMarkers
    }
}

function setStopCodeValue(value) {
    $("gtfs_stop_code").value = value;
}

$("#tripId").click(function (e) {
    removeTripFromMap(e.currentTarget.attributes['tripid'].value);
});


function initMap() {
    //const mapid = document.getElementById('mapid');
    // mymap is declared in script in solution.html
    // here it is initialized
    //mymap = L.map(mapid, {
    mymap = L.map('mapid', {
        // options can go in here
        zoomControl: true,
        attributionControl: false,
    }).setView(
        [31.738494, 34.995529], // center is set here
        14);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 18,
    }).addTo(mymap);
    return mymap;
}

$(document).ready(function () {
    allTripsFromJs = allTrips; // allTrips arrives from including allTrips.js. In production will arrive from Python analysis of Siri Data
    clog("allTripsFromJs initialized to allTrips (content of js file)")
    const sampleVehicleId = allTripsFromJs.gtfsTrips[0].vehicleId;
    clog("vehicleId of first trip is " + sampleVehicleId);

    //mymap = initMap();    // done in solution.html

    /*
    returns this object - can be used to remove this route from map
    {
        route: polyline,
        stops: stopsMarkers,
        siri: siriMarkers
    }
    */
    // moved to solutionWrapper.js
    // const numberOfTrips = allTripsFromJs.gtfsTrips.length;
    // clog("gtfs trips: " + numberOfTrips);
    // populateTripsGrid(allTripsFromJs);

    //const route1 = displayAll(allTripsFromJs.gtfsTrips[1], 'black');
    //mapAllRoutesDisplayed.set(allTripsFromJs.gtfsTrips[1].tripId, route1);

});