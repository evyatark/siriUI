//var mymap ;   - declared in script in solution.html
var mapAllRoutesDisplayed = new Map();
var allTripsFromJs ; // initialized in solution.html ready function, to content of allTrips
var displayStatusDefaultValue = {
    shape: false,
    stops: false,
    siri: false
};
var displayStatus;

const log = true;
function clog(arg) {
    if (log) {
        console.log(arg);
    }
}

function toggleStops(tripId) {
    if (!tripId) return;
    clog("toggle Stops");
    if (!parent.displayOptionsInWrapper.displayStops) {
    //if (displayStatus.stops) {
        clog("remove...");
        undisplayTrip(tripId, true, false);
    }
    else {
        clog("add...");
        let routeObj = mapAllRoutesDisplayed.get(tripId);
        if (!routeObj) return;
        displayMarkers(routeObj.stops);
        displayStatus.stops = true;
    }
    clog("toggle Stops done");
}

function toggleVisibilityOfTooltip(displaySiriTooltipAlways) {
    if (displaySiriTooltipAlways) {
        clog("toggleVisibilityOfTooltip (solution.js) visible");
        $('.icon-text').attr('style', 'visibility: visible;');
    }
    else {
        clog("toggleVisibilityOfTooltip (solution.js) hidden");
        $('.icon-text').attr('style', 'visibility: hidden;');
    }
}

function toggleSiri(tripId) {
    if (!tripId) return;
    clog("toggle Siri");
    if (!parent.displayOptionsInWrapper.displaySiri) {
    //if (displayStatus.siri) {
        clog("remove...");
        undisplayTrip(tripId, false, true);
    }
    else {
        clog("add...");
        let routeObj = mapAllRoutesDisplayed.get(tripId);
        if (!routeObj) return;
        displayMarkers(routeObj.siri);
        displayStatus.siri = true;
    }
    clog("toggle Siri done");
}

function toggleShape(tripId) {
    if (!tripId) return;
    clog("toggle Shape");
    if (!parent.displayOptionsInWrapper.displayShape) {
    //if (displayStatus.shape) {
        clog("remove...");
        undisplayShape(tripId);
    }
    else {
        clog("add...");
        displayShape(tripId);
    }
    clog("toggle Shape done");
}

function displayRouteOnMap(shape, color1) {
    clog('displaying shape...  (' + shape.length + ' points)');
    // mymap is declared in script in solution.html
    const origWeight = 3;
    const polyline = L.polyline(shape, {color: color1, weight: origWeight}).addTo(mymap);
    displayStatus.shape = true;
    //clog('polyline added to map');
    polyline.on('mouseover',function (ev) {
        ev.target.setStyle({weight: 7});
    });
    polyline.on('mouseout',ev => ev.target.setStyle({weight: origWeight})
    );
    //     mymap.fitBounds(polyline.getBounds());
    return polyline;
}

function createSiriPopupText(siriPoint) {
    let html = "<b>time:</b>" + siriPoint.properties.time_recorded + "<br/>";
    html = html+ "<b>lat/lon:</b>" + siriPoint.geometry.coordinates;
    return "POPUP " + html;
}

function createSiriTitleText(siriPoint) {
    let html = "time:" + siriPoint.properties.time_recorded;
    return "TITLE "+html;
}

function removeLeadingZero(str) {
    if ((str) && (str[0] == '0')) {
        str = str[1];
    }
    return str;
}

function onlyHour(fullOad, date) {
    let oad = fullOad;
    if ((date && (fullOad.includes(date) && fullOad.includes("T"))) ||
        (!date && fullOad.includes("T")) )
    {
        oad = fullOad.split("T")[1];
        let temp = oad.split(":");
        if (temp.length == 3) {
            oad = removeLeadingZero(temp[0]) + ":" + temp[1] + ":" + temp[2];
        }
        else {
            oad = removeLeadingZero(temp[0]) + ":" + temp[1];
        }
    }
    return oad;
}


function createSiriTooltipText(siriPoint, date) {
    let fullTime = siriPoint.properties.time_recorded;
    let hourOnly = onlyHour(fullTime, date);

    let distance = siriPoint.properties.distanceFromStart;
    let distanceStr = " (unknown distance)";
    if (distance) {
        distanceStr = " (distance=" + distance + ")";
    }
    return hourOnly + distanceStr;

    // let html = "time:" + siriPoint.properties.time_recorded;
    // return "TOOLTIP "+html;
}

function siriMarkerOnClick(e)
{
    // this is in addition to the popup that is opened when clicking on the marker.
    // first this function is executed, then the popup is opened
    clog("you clicked the marker at " + e.latlng);// + ". title of marker: " + this.options.title + ". name: " + this.options.name);
    e.target.getPopup().openPopup();
}

function tooltipOptions() {
    let tooltipOptions = {};
    if (parent.displayOptionsInWrapper.displaySiriTooltipAlways) {
        tooltipOptions = {permanent: true};
    }
    return tooltipOptions;
}

function getMarkerWithTextIcon(theText, coordinates) {
    // <div class="icon-text"
    // add attribute style="visibility: visible;" to div that has class = "icon-text"
    // to hide - change visible to hidden
    let icon = new L.TextIcon({ text: theText, color: 'red'});
    let marker = L.marker(coordinates, { icon: icon });
    return marker;
}

function displaySiriPointsOnMap(siriPoints, iconFileName, date) {
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
    //let siriPopup = "<p><b>time:</b>" + siriPoint.properties.time_recorded + "</p>" + "<p><b>lat/lon:</b>" + siriPoint.geometry.coordinates + "</p>"
    clog("siriPoints array has size " + siriPoints.length);
    let popupOptions = {autoClose: false, closeOnClick: false}; // options meant to leave popup open
    const siriMarkers = siriPoints.map(siriPoint => {
            let text = onlyHour( siriPoint.properties.time_recorded );
            const coordinates = siriPoint.geometry.coordinates;
            const markerSpecial = getMarkerWithTextIcon(text, coordinates);
            //const title = createSiriTitleText(siriPoint);

            // const marker = L.marker(coordinates, {
            //     icon: iconYellow,
            //     //title: title,
            //     riseOnHover: true
            // })

            let marker = markerSpecial
                .on('click', siriMarkerOnClick)
                    .addTo(mymap);
                // title and tooltip are different (title is a browser thing, tooltip is leaflet thing(?)
                // but if both defined, they will be displayed one above the other

                // disable this tooltip in favor of icon-text with visibility toggling
                if (false == parent.displayOptionsInWrapper.displaySiriTooltipAlways) {
                    clog("binding tooltip");
                    clog(parent.displayOptionsInWrapper);
                    marker = marker.bindTooltip(createSiriTooltipText(siriPoint, date), tooltipOptions());
                }

                //.bindPopup(createSiriPopupText(siriPoint), popupOptions)

            // + siriPoint.timestamp + siriPoint.recalculatedETA
        // + calculate distance from point to nearest stop, or
        // calculate the distance on the shape from point to nearest stop, or
        // if the point is near enough to any stop, compare timestamp+ETA to those of the stop
        // calculate if the point is on the shape (if not - is it near enough), or
        // if it is on the shape, calculate the distance from stop#1 on the shape
            // marker.on('mouseover', function(e) {
            //     let sp = siriPoint.properties;
            //     parent.document.getElementById('siri_current_time').value = 'Time: '+sp.time_recorded;
            //     parent.document.getElementById('siri_line_name').value = "קו 149";//sp.properties.missing;
            //     parent.document.getElementById('siri_vehicle_id').value = "vehicle Id: 3174039";//sp.properties.missing;
            //     parent.document.getElementById('siri_departure').value = "יצא ב 06:14";//sp.properties.missing;
            //     parent.document.getElementById('siri_estimated_arrival').value = "אמור להגיע ב 07:14";//sp.properties.missing;
            // }).on('mouseout', clearSiriPointDisplay);

            //marker.getPopup().autoClose=false;
            //marker.getPopup().openOn(mymap);
            marker.openPopup();
            return marker;
        });
        // add constant tooltips for all siri points (if displaySiriTooltipAlways == true)
        toggleVisibilityOfTooltip(parent.displayOptionsInWrapper.displaySiriTooltipAlways);

    return siriMarkers;
}



function displayStopsOnMap(stops, stopIconFileName) {
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
    if (!stopIconFileName) {
        stopIconFileName = 'yellow-flag.svg';
    }
    let iconYellow = L.icon({
        iconUrl: './icons/' + stopIconFileName,
        iconSize: 30
    });
    const stopsMarkers = stops.features.map(stop => {
            const popupText = preparePopup(stop.properties)
            const coordinates = stop.geometry.coordinates;
            const marker = L.marker(coordinates, {icon: iconYellow}).addTo(mymap);//.bindPopup(preparePopup(stop.properties));
            marker.on('mouseover', function (e) {
                e.target.bindPopup(popupText);
                //let x = parent.document.getElementById('gtfs_stop_code');
                clog("mouse over stop " + stop.properties.stop_sequence);
                // gtfs_stop_code in the parent document -
                //works in Firefox but not in Chrome
                // in Chrome use https://www.thegeekstuff.com/2016/09/disable-same-origin-policy/
                //x.placeholder=stop.properties.stop_code;      // possible to change placeholder text
                // parent.document.getElementById('gtfs_stop_code').value = stop.properties.stop_code;
                // parent.document.getElementById('gtfs_stop_id').value = stop.properties.stop_id;
                // parent.document.getElementById('gtfs_stop_name').value = "תחנה בזמן";//stop.properties.stop_name;
            }).on('mouseout' , function (e) {
                clog("mouse out " + e);
                e.target.closePopup();
                //clearStopDisplay
            });
            return marker;
        }
    );
    return stopsMarkers;
}

function preparePopup(stopProp) {
    if (!stopProp) return "unknown";
    let plannedTime = "<br>planned arrival/departure:" + stopProp.arrivalTime;
    if (stopProp.arrivalTime != stopProp.departureTime) {
        plannedTime = "<br>planned arrival: " + stopProp.arrivalTime + "<br>planned departure:" + stopProp.departureTime;
    }
    const s = "<b>" +
        "stop #" + stopProp.stop_sequence +    //stopProp.trip_id
        "<br>stop name:</b>" + stopProp.stop_name +
        "<br><b>description:</b>" + stopProp.stop_desc + //stop_id
        "<br><b>stop code:</b>" + stopProp.stop_code +
        "<br><b>distance:</b>" + stopProp.distance +
        plannedTime;
    return s;
}



function fetchMeasuredDistance(stop1, stop2, routeId, date) {
    let sequence1 = stop1.properties.stop_sequence
    let c = stop1.geometry.coordinates[0];
    let sequence1lat = stop1.geometry.coordinates[0];
    let sequence1lon = stop1.geometry.coordinates[1];
    let sequence2 = stop2.properties.stop_sequence
    let sequence2lat = stop2.geometry.coordinates[0];
    let sequence2lon = stop2.geometry.coordinates[1];

    clog("called fetchMeasuredDistance, from=" + stop1 + ", sequence=" + sequence1+ ", to=" + stop2 + ", sequence=" + sequence2+ ", routeId=" + routeId + ", date=" + date);
    const url = 'gtfs/distance/calc/' + sequence1lat + "/" + sequence1lon + "/" + sequence2lat + "/" + sequence2lon + "/" + routeId + "/" + date;
    $.ajax({
        url: url
    })
        .done(function( json ) {
            let retVal = json; // returned value is just a string  //JSON.parse(json);
            clog("retVal=" + retVal);
            // populate text box v5 with the result
            parent.document.getElementById('v5').value = retVal;
        });
}

function measureDistanceBetweenStops(stops) {
    let x = stops.features; // array
    let cs = x[0].geometry.coordinates;
    let lat = cs[0];
    let lon = cs[1];
    let y = x[0].properties.stop_sequence;  // starts from 1, x.gtfsTrips[""0""].stops.features[""0""].properties.arrivalTime
    let d = x[0].properties.distance;   // x.gtfsTrips[""0""].stops.features[""0""].properties.distance
    let from = parent.document.getElementById('v3').value;
    let to = parent.document.getElementById('v4').value;
    if (from && to) {
        clog("from=" + from + ", to=" + to);
        let myRouteId = sessionStorage.getItem("selectedRouteId");
        // TODO possibly need to find in gtfsTrips array an element that has the current Route
        let mystops = stops.features;
        let stop1 = mystops[from-1];
        let sequence1 = stop1.properties.stop_sequence
        let sequence1lat = stop1.geometry.coordinates[0];
        let sequence1lon = stop1.geometry.coordinates[1];
        let stop2 = mystops[to-1];
        let sequence2 = stop2.properties.stop_sequence
        let sequence2lat = stop2.geometry.coordinates[0];
        let sequence2lon = stop2.geometry.coordinates[1];

        fetchMeasuredDistance(stop1, stop2, myRouteId, sessionStorage.getItem("selectedDate"));
    }
    else {
        clog("something wrong. Probably no value was typed in one of the text boxes");
    }
}

// function displayJustShape() {
//     let shapeJson = sessionStorage.getItem("shapeOfSelectedRoute");
//     if (shapeJson) {
//         let shape = { "coordinates": JSON.parse(shapeJson) } ;
//         let polyline = displayRouteOnMap(shape.coordinates, 'red');
//     }
// }

// arg gtfsTripObject is an object of the format in allTrips
function askDisplayAll(gtfsTripObject, setView) {

    /*
    gtfsTripObject {
        agencyCode: "16"
        agencyName: "Superbus"
        alternateTripId: "42315655"
        date: "2019-11-05"
        dayOfWeek: "2"
        dns: null
        gtfsETA: null
        gtfsTripId: null
        originalAimedDeparture: "2019-11-05T05:35:00"
        routeId: "15527"
        shortName: "415"
        siri: {type: "FeatureCollection", features: Array(34)}
        siriTripId: "42315657"
        stops: {type: "FeatureCollection", features: Array(37)}
        suspicious: false
        vehicleId: "6690232"

     */
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
    // object added to map looks like:
    // {
    //     routeId: tripObject.routeId,
    //         tripId: tripObject.siriTripId,
    //     dns: tripObject.dns,
    //     route: polyline,
    //     stops: stopsMarkers,
    //     siri: siriMarkers
    // }
    mapAllRoutesDisplayed.set(tripId, route1);
    clog("added. map now contains " + mapAllRoutesDisplayed.size);
    toggleVisibilityOfTooltip(parent.displayOptionsInWrapper.displaySiriTooltipAlways);
    if (setView && gtfsTripObject.siri && gtfsTripObject.siri.features) {
        mymap.setView(gtfsTripObject.siri.features[0].geometry.coordinates, 12);
    }
}

function displayMarkers(markers) {
    if (!markers) return;
    markers.map(marker =>
        marker.addTo(mymap)
    );
}

function undisplayMarkers(markers) {
    if (!markers) return;
    markers.map(marker =>
        marker.removeFrom(mymap)
    );
}

function undisplayStops(stops) {
    undisplayMarkers(stops);
}

function undisplaySiriMarkers(siri) {
    undisplayMarkers(siri);
}

function undisplayShape(tripId) {
    let routeObj = mapAllRoutesDisplayed.get(tripId);
    if (!routeObj) return;
    if (routeObj.route) {
        clog("removing polyline...");
        try {
            routeObj.route.removeFrom(mymap);   //: polyline,
        }
        catch (e) {
            clog(e);
        }
        displayStatus.shape = false;
        clog("removed.");
    }
}

function displayShape(tripId) {
    let routeObj = mapAllRoutesDisplayed.get(tripId);
    if (!routeObj) return;
    if (routeObj.route) {
        clog("adding polyline...");
        routeObj.route.addTo(mymap);   //: polyline,
        displayStatus.shape = true;
        clog("added.");
    }
}


function undisplayTrip(tripId, doStops, doSiri) {
    let routeObj = mapAllRoutesDisplayed.get(tripId);
    if (!routeObj) return;
    if (doStops) {
        undisplayStops(routeObj.stops);
        displayStatus.stops = false;
    }
    if (doSiri) {
        undisplaySiriMarkers(routeObj.siri);
        displayStatus.siri = false;
    }
}


function displayAll(mymap, tripObject, color) {
    if (!color) {
        color = 'red';      // color of the markers
    }
    let polyline = {};
    if (tripObject.shape) {
        polyline = displayRouteOnMap(tripObject.shape.coordinates, 'red');    // color of the polyline
        displayStatus.shape = true;
    }
    let stopsMarkers = {};
    if (tripObject.stops) {
        stopsMarkers = displayStopsOnMap(tripObject.stops, "yellow-flag.svg");
        displayStatus.stops = true;
    }
    let siriMarkers = null;
    if (tripObject.siri) {
        siriMarkers = displaySiriPointsOnMap(tripObject.siri.features, color + '-pin.svg', tripObject.date);   // color of the markers
        displayStatus.siri = true;
    }
    return {
        routeId: tripObject.routeId,
        tripId: tripObject.siriTripId,
        dns: tripObject.dns,
        route: polyline,
        stops: stopsMarkers,
        siri: siriMarkers
    };
}

function removeTripFromMap(tripId) {
    if (tripId) {
        clog("removing trip tripId " + tripId + " from display...");
        // object retrieved from map looks like:
        // {
        //     routeId: tripObject.routeId,
        //     tripId: tripObject.siriTripId,
        //     dns: tripObject.dns,
        //     route: polyline,
        //     stops: stopsMarkers,
        //     siri: siriMarkers
        // }
        const tripObject = mapAllRoutesDisplayed.get(tripId);
        clog("trip object=" + tripObject);
        if (tripObject) {
            removeAll(tripObject);
            clog("remove tripId " + tripId);
        }
    }
}

function removeAll(tripObject) {
    if (tripObject) {
        clog("removing from display markers of trip " + tripObject.tripId);
        if (tripObject.dns) {
            clog("that trip was DNS, so nothing to remove?");
            //return;
        }
        if (tripObject.route) {
            clog("removing polyline...");
            try {
                routeObj.route.removeFrom(mymap);   //: polyline,
            }
            catch (e) {
                clog(e);
            }
            displayStatus.shape = false;
            clog("removed.");
        }
        if (tripObject.stops && tripObject.stops.length && (tripObject.stops.length > 0)) {  //: stopsMarkers,
            clog("removing stop markers ...");
            tripObject.stops.forEach(stop => stop.removeFrom(mymap));
            displayStatus.stops = false;
            clog("removed.");
        }
        if (tripObject.siri) {
            clog("removing siri markers ...");
            tripObject.siri.forEach(point => point.removeFrom(mymap));    //: siriMarkers
            displayStatus.siri = false;
            clog("removed.");
        }
    }
}

// function setStopCodeValue(value) {
//     $("gtfs_stop_code").value = value;
// }
//
// $("#tripId").click(function (e) {
//     removeTripFromMap(e.currentTarget.attributes['tripid'].value);
// });


function initMap() {
    //const mapid = document.getElementById('mapid');
    if (mymap) {
        clog("undoing previous map");
        mymap.off();
        mymap.remove();
    }
    displayStatus = displayStatusDefaultValue;
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
    displayStatus = displayStatusDefaultValue;
    allTripsFromJs = allTrips; // allTrips arrives from including allTrips.js. In production will arrive from Python analysis of Siri Data
    clog("allTripsFromJs initialized to allTrips (content of js file)");
    const sampleVehicleId = allTripsFromJs.gtfsTrips[0].vehicleId;
    clog("vehicleId of first trip is " + sampleVehicleId);

});